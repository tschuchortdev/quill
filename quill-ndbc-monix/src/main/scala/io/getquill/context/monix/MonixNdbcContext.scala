package io.getquill.context.monix

import java.sql.{Array => _}
import java.util.concurrent.Executors
import java.util.function.Supplier

import io.getquill.context.ndbc.NdbcContextBase
import io.getquill.context.sql.idiom.SqlIdiom
import io.getquill.util.ContextLogger
import io.getquill.{NamingStrategy, ReturnAction}
import io.getquill.ndbc.TraneFutureConverters._
import io.trane.future.scala.{Await, Future, toScalaFuture}
import io.trane.future.{FuturePool, Future => JFuture}
import io.trane.ndbc.{DataSource, PreparedStatement, Row}
import monix.eval.Task
import monix.execution.Scheduler
import monix.reactive.Observable

import scala.concurrent.Promise
import scala.concurrent.duration.Duration

object MonixNdbcContext {
  trait Runner {
    def schedule[T](t: Task[T]): Task[T]
    def schedule[T](o: Observable[T]): Observable[T]
  }

  object Runner {
    def using(scheduler: Scheduler) = new Runner {
      override def schedule[T](t: Task[T]): Task[T] = t.executeOn(scheduler, forceAsync = true)
      override def schedule[T](o: Observable[T]): Observable[T] = o.executeOn(scheduler, forceAsync = true)
    }
}

/**
 * Quill context that wraps all NDBC calls in `monix.eval.Task`.
 *
 */
abstract class MonixNdbcContext[Dialect <: SqlIdiom, Naming <: NamingStrategy, P <: PreparedStatement, R <: Row](
  dataSource: DataSource[P, R],
  runner:     Runner
) extends MonixContext[Dialect, Naming]
  with NdbcContextBase[Dialect, Naming, P, R] {

  override private[getquill] val logger = ContextLogger(classOf[MonixNdbcContext[_, _, _, _]])

  override type RunActionResult = Long
  override type RunActionReturningResult[T] = T
  override type RunBatchActionResult = List[Long]
  override type RunBatchActionReturningResult[T] = List[T]

  override implicit protected val resultEffect = new NdbcContextBase.ContextEffect[Task] {
    override def wrapAsync[T](f: (Complete[T]) => Unit): Task[T] = Task.deferFuture {
      val p = Promise[T]()
      f(p.complete)
      p.future
    }

    override def flatMap[A, B](a: Task[A])(f: A => Task[B]): Task[B] = a.flatMap(f)

    override def runBlocking[T](eff: Task[T], timeout: Duration): T = {
      import monix.execution.Scheduler.Implicits.global
      eff.runSyncUnsafe(timeout)
    }

    override def wrap[T](t: => T): Task[T] = Task.apply(t)

    override def fmap[A, B](fa: Task[A])(f: A => B): Task[B] = fa.map(f)

    override def sequence[T](f: List[Task[T]]): Task[List[T]] = Task.sequence(f)
  }

  import runner._

  // Need explicit return-type annotations due to scala/bug#8356. Otherwise macro system will not understand Result[Long]=Task[Long] etc...
  override def executeAction[T](sql: String, prepare: Prepare = identityPrepare): Task[Long] =
    super.executeAction(sql, prepare)

  override def executeQuery[T](sql: String, prepare: Prepare = identityPrepare, extractor: Extractor[T] = identityExtractor): Task[List[T]] =
    super.executeQuery(sql, prepare, extractor)

  override def executeQuerySingle[T](sql: String, prepare: Prepare = identityPrepare, extractor: Extractor[T] = identityExtractor): Task[T] =
    super.executeQuerySingle(sql, prepare, extractor)

  override def executeActionReturning[O](sql: String, prepare: Prepare = identityPrepare, extractor: Extractor[O], returningBehavior: ReturnAction): Task[O] =
    super.executeActionReturning(sql, prepare, extractor, returningBehavior)

  override def executeBatchAction(groups: List[BatchGroup]): Task[List[Long]] =
    super.executeBatchAction(groups)

  override def executeBatchActionReturning[T](groups: List[BatchGroupReturning], extractor: Extractor[T]): Task[List[T]] =
    super.executeBatchActionReturning(groups, extractor)

  override def close(): Unit = dataSource.close()

  override protected def withDataSource[T](f: DataSource[P, R] => Task[T]): Task[T] =
    schedule(f(dataSource))

  /* TODO: I'm assuming that we don't need to bracket and close the dataSource like with JDBC
        because previously it wasn't done either */

  protected def withDataSourceObservable[T](f: DataSource[P, R] => Observable[T]): Observable[T] =
    schedule(f(dataSource))

  /* TODO: I'm assuming that we don't need to turn autocommit off/on for streaming because I can't
        find any way to do so with the NDBC DataSource and it seems to handle streaming on its own */

  def transaction[T](f: => Task[T]): Task[T] = withDataSource { ds =>
    implicit def javaSupplier[S](s: => S): Supplier[S] = new Supplier[S] {
      override def get = s
    }

    val javaFuturePool = FuturePool.apply(Executors.newCachedThreadPool())

    Task.deferFutureAction(implicit scheduler =>
      javaFuturePool.isolate(
        ds.transactional {
          f.runToFuture: JFuture[T]
        }
      )
    )
  }

  def streamQuery[T](fetchSize: Option[Index], sql: String, prepare: Prepare = identityPrepare, extractor: Extractor[T] = identityExtractor): Observable[T] =
    Observable
      .eval {
        // TODO: Do we need to set ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY?
        val stmt = createPreparedStatement(sql)
        val (params, ps) = prepare(stmt)
        logger.logQuery(sql, params)
        ps
      }
      .flatMap(ps => withDataSourceObservable { ds =>
        Observable.fromReactivePublisher(ds.stream(ps))
      })
      .map(extractor)
}
