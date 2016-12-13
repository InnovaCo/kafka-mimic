package eu.inn.kafka.mimic

import java.util
import java.util.concurrent.Executors
import java.util.regex.Pattern

import akka.actor.{Actor, ActorRef, Kill, PoisonPill, Props}
import kafka.consumer.{Consumer, ConsumerConfig, Whitelist}
import kafka.javaapi.consumer.ConsumerConnector
import kafka.utils.ZkUtils
import org.I0Itec.zkclient.{IZkChildListener, IZkDataListener}

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success, Try}
import cats.syntax.either._

trait CoordinatorComponent {

  this: ConfigComponent
    with ActorSystemComponent
    with MetricsComponent
    with WorkerComponent
    with PublisherComponent ⇒

  lazy val coordinator = actorSystem.actorOf(Props(classOf[CoordinatorActor], this), "coordinator")

  class CoordinatorActor extends Actor with KafkaHelpers with Logging {

    private val ioExecutor = ExecutionContext.fromExecutor(Executors.newCachedThreadPool()) // todo: name threads

    private val workers = (0 until appConfig.workersCount) map (i ⇒ buildWorker(s"worker-$i"))

    private val sourceZk = buildZkClient(appConfig.sourceZkConfig)
    private val targetZk = buildZkClient(appConfig.targetZkConfig)

    private var connector: Option[ConsumerConnector] = None

    private val outstandingPolls = mutable.Set.empty[ActorRef]

    def receive = {
      case Start ⇒
        log.info("Starting coordinator.")
        context.become(reassigning.orElse(defaultBehavior))
        self ! Reassign

      case x ⇒
        log.error(s"Unexpected message: $x")
    }

    private def reassigning: Receive = {
      case Reassign ⇒
        Future {
          metrics.timer("reassigning").time { reassign() }
        }(ioExecutor) onComplete {
          case Success(Right(_)) ⇒
            context.become(consuming.orElse(defaultBehavior))
            broadcastPolls()
            context.system.scheduler.scheduleOnce(30.seconds, self, RequestSync)
          case Success(Left(msg)) ⇒
            log.error(msg)
            self ! Shutdown
          case Failure(e) ⇒
            log.error("Unable to reassign. Retrying in 30 secs.", e)
            context.system.scheduler.scheduleOnce(30.seconds, self, Reassign)
        }

      case SyncCompleted ⇒
        self ! Reassign
    }

    private def consuming: Receive = {
      case BatchCompleted ⇒
        sender ! NextBatch

      case RequestSync ⇒
        context.become(syncing.orElse(defaultBehavior))
    }

    private def shuttingDown: Receive = {
      case RequestShutdown ⇒
        log.trace("Already shutting down.")

      case RequestReassign ⇒
        log.trace("Shutting down.")

      case SyncCompleted ⇒
        self ! Shutdown
    }

    private def syncing: Receive = {
      case BatchCompleted ⇒
        outstandingPolls -= sender
        if (outstandingPolls.isEmpty) {
          Future {
            metrics.timer("flushing").time {
              publisher.waitForCompletion()
            }
          }(ioExecutor) onComplete {
            case Success(_) ⇒
              commit()
              self ! SyncCompleted
            case Failure(e) ⇒
              log.info("Unable to send messages. Shutting down...", e)
              self ! Shutdown
          }
        }

      case SyncCompleted ⇒
        context.become(consuming.orElse(defaultBehavior))
        broadcastPolls()
        context.system.scheduler.scheduleOnce(30.seconds, self, RequestSync)
    }

    private def defaultBehavior: Receive = {
      case RequestReassign ⇒
        log.trace("Requesting reassign.")
        context.become(reassigning.orElse(syncing).orElse(defaultBehavior))

      case RequestShutdown ⇒
        log.trace("Requesting shutdown")
        context.become(shuttingDown.orElse(syncing).orElse(defaultBehavior))
        self ! RequestSync

      case Shutdown ⇒
        log.info("Shutting down now.")
        sourceZk.unsubscribeAll()
        workers.foreach(_ ! PoisonPill)
        publisher.close()
        sourceZk.close()
        targetZk.close()
        connector.foreach(_.shutdown())
        context.system.terminate()
        Future {
          Try(Await.ready(context.system.whenTerminated, 15 seconds))
          sys.exit(0)
        }(ioExecutor)

      case e: Throwable ⇒
        log.error("Unexpected error. Shutting down.", e)
        self ! Shutdown

      case Error(e, worker) ⇒
        log.error(s"Unexpected error in $worker. Shutting down.", e)
        self ! Shutdown
    }

    private def hasCyclicRoutes(topicMap: Map[String, String]) =
      topicMap.exists { case (f, t) ⇒ f == t } &&
        appConfig.sourceZkConfig.connectionString == appConfig.targetZkConfig.connectionString

    private def reconnectToKafka() = {
      connector.foreach { c =>
        c.shutdown()
        connector = None
      }
      connector = Some(Consumer.createJavaConsumerConnector(new ConsumerConfig(appConfig.consumerConfig)))
      Right(connector)
    }

    private def createTopicMap() = {
      val topicMap = buildTopicsMap(sourceZk, appConfig.whitelist, appConfig.blacklist, appConfig.prefix)
      if (hasCyclicRoutes(topicMap)) {
        Left("Cannot use same topics for consuming/producing within same kafka cluster")
      } else {
        Right(topicMap)
      }
    }

    private def syncTopicsConfig(topicMap: Map[String, String]) = {
      log.info("Syncing topics config")
      topicMap foreach { case (s, t) ⇒
        syncTopicConfig(s, sourceZk, t, targetZk,
          appConfig.partitioner.isInstanceOf[KeepingPartitionPartitioner], appConfig.replicationFactor)
      }
    }

    private def createConsumerJobs(topicMap: Map[String, String]) = {
      val topicFilter = Whitelist("^(%s)$".format(topicMap.keys.map(Pattern.quote).mkString("|")))
      connector match {
        case Some(c) ⇒
          Right(
            c.createMessageStreamsByFilter(topicFilter, appConfig.workersCount)
              .map(s ⇒ Job(s, topicMap, appConfig.skipCorrupted))
          )
        case None ⇒
          Left("Connector is not ready")
      }
    }

    private def subscribeToConfigChanges(topicMap: Map[String, String]) = {
      sourceZk.subscribeChildChanges(ZkUtils.TopicConfigPath, new IZkChildListener {
        override def handleChildChange(parentPath: String, currentChildren: util.List[String]): Unit = {
          log.info(s"Received zk topics update at $parentPath")
          self ! RequestReassign
        }
      })

      topicMap.keys foreach { t ⇒
        sourceZk.subscribeDataChanges(ZkUtils.TopicConfigPath + "/" + t, new IZkDataListener {
          override def handleDataChange(dataPath: String, data: scala.Any): Unit = {
            log.info(s"Received zk topic config change at $dataPath")
            self ! RequestReassign
          }

          override def handleDataDeleted(dataPath: String): Unit = {
            log.info(s"Received zk topic deletion at $dataPath")
            self ! RequestReassign
          }
        })
      }
    }

    private def reassign() = {
      log.info("Reassigning workers jobs.")

      sourceZk.unsubscribeAll()

      for {
        _ <- reconnectToKafka()
        topicMap <- createTopicMap()
        _ = syncTopicsConfig(topicMap)
        jobs <- createConsumerJobs(topicMap)
        _ = subscribeToConfigChanges(topicMap)
      } yield {
        workers.zip(jobs) foreach { case (w, a) ⇒
          log.trace(s"Assigning $a to $w")
          w ! Assignment(a)
        }
      }
    }

    private def commit() = {
      log.debug("Committing.")
      metrics.timer("committing").time {
        connector.foreach(_.commitOffsets(true))
      }
    }

    private def broadcastPolls() = {
      log.debug(s"Broadcasting polls.")
      workers foreach { w ⇒
        outstandingPolls += w
        w ! NextBatch
      }
    }
  }

}