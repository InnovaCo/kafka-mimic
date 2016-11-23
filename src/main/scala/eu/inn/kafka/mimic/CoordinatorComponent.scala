package eu.inn.kafka.mimic

import java.util
import java.util.concurrent.Executors
import java.util.regex.Pattern

import akka.actor.{Actor, ActorRef, Kill, Props}
import eu.inn.util.Logging
import eu.inn.util.metrics.StatsComponent
import kafka.consumer.{Consumer, ConsumerConfig, Whitelist}
import kafka.javaapi.consumer.ConsumerConnector
import kafka.utils.ZkUtils
import org.I0Itec.zkclient.{IZkChildListener, IZkDataListener}

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success, Try}


trait CoordinatorComponent {

  this: AppConfigComponent
    with ActorSystemComponent
    with StatsComponent
    with WorkerComponent
    with PublisherComponent ⇒

  lazy val coordinator = actorSystem.actorOf(Props(classOf[CoordinatorActor], this), "coordinator")

  // todo: extract
  lazy val stats = createStats(s"kafka-mimic.coordinator")

  class CoordinatorActor extends Actor with KafkaHelpers with Logging {

    private val ioExecutor = ExecutionContext.fromExecutor(Executors.newCachedThreadPool()) // todo: name threads

    val workers = (0 until appConfig.workersCount) map (i ⇒ buildWorker(s"worker-$i"))

    val sourceZk = buildZkClient(appConfig.sourceZkConfig)
    val targetZk = buildZkClient(appConfig.targetZkConfig)

    var connector: ConsumerConnector = null // todo: beautify

    val outstandingPolls = mutable.Set.empty[ActorRef]

    def receive = {

      case Start ⇒
        log.info("Starting coordinator.")
        context.become(reassigning.orElse(defaultBehavior))
        self ! Reassign

      case x ⇒
        log.error(s"Unexpected message: $x")
    }

    def reassigning: Receive = {
      case Reassign ⇒
        Future {
          stats.timer("reassigning") { reassign() }
        }(ioExecutor) onComplete {
          case Success(_) ⇒
            context.become(consuming.orElse(defaultBehavior))
            broadcastPolls()
            context.system.scheduler.scheduleOnce(30.seconds, self, RequestSync)
          case Failure(e) ⇒
            log.error("Unable to reassign. Retrying in 30 secs.", e)
            context.system.scheduler.scheduleOnce(30.seconds, self, Reassign)
        }

      case SyncCompleted ⇒
        self ! Reassign
    }

    def consuming: Receive = {
      case BatchCompleted ⇒
        sender ! NextBatch

      case RequestSync ⇒
        context.become(syncing.orElse(defaultBehavior))
    }

    def shuttingDown: Receive = {
      case RequestShutdown ⇒
        log.trace("Already shutting down.")

      case RequestReassign ⇒
        log.trace("Shutting down.")

      case SyncCompleted ⇒
        self ! Shutdown
    }

    def syncing: Receive = {
      case BatchCompleted ⇒
        outstandingPolls -= sender
        if (outstandingPolls.isEmpty) {
          Future {
            stats.timer("flushing") {
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

    def defaultBehavior: Receive = {
      case RequestReassign ⇒
        log.trace("Requesting reassign.")
        context.become(reassigning.orElse(syncing).orElse(defaultBehavior))

      case RequestShutdown ⇒
        log.trace("Requesting shutdown")
        context.become(shuttingDown.orElse(syncing).orElse(defaultBehavior))
        self ! RequestSync

      case Shutdown ⇒
        log.info("Shutting down now.")
        workers.foreach(_ ! Kill)
        publisher.close()
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

    def reassign() = {
      log.info("Reassigning workers jobs.")

      sourceZk.unsubscribeAll()

      if (connector != null) {
        connector.shutdown()
        connector = null
      }

      connector = Consumer.createJavaConsumerConnector(new ConsumerConfig(appConfig.consumerConfig))

      val topicsMap = buildTopicsMap(sourceZk, appConfig.whitelist, appConfig.blacklist, appConfig.prefix)

      log.info("Syncing topics config")
      topicsMap foreach { case (s, t) ⇒ syncTopicConfig(s, sourceZk, t, targetZk, appConfig.partitioner.isInstanceOf[KeepingPartitionPartitioner], appConfig.replicationFactor) }

      val topicFilter = new Whitelist("^(%s)$".format(topicsMap.keys.map(Pattern.quote).mkString("|")))

      val jobs = connector
        .createMessageStreamsByFilter(topicFilter, appConfig.workersCount)
        .map(s ⇒ Job(s, topicsMap, appConfig.skipCorrupted))

      workers.zip(jobs) foreach { case (w, a) ⇒
        log.trace(s"Assigning $a to $w")
        w ! Assignment(a)
      }

      sourceZk.subscribeChildChanges(ZkUtils.TopicConfigPath, new IZkChildListener {
        override def handleChildChange(parentPath: String, currentChilds: util.List[String]): Unit = {
          log.info(s"Received zk topics update at $parentPath")
          self ! RequestReassign
        }
      })

      topicsMap.keys foreach { t ⇒
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

    def commit() = {
      log.debug("Committing.")
      stats.timer("committing") {
        connector.commitOffsets(true)
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