package io.getquill.context.ndbc

import java.util.concurrent.Executors
import java.util.function.Supplier

import cats._
import io.getquill.context.ndbc.NdbcContextBase.ContextEffect
import io.getquill.context.sql.idiom.SqlIdiom
import io.getquill.{NamingStrategy, ReturnAction}
import io.getquill.ndbc.TraneFutureConverters._
import io.trane.future.scala.{Await, Future, Promise, toJavaFuture, toScalaFuture}
import io.trane.future.{FuturePool, Future => JFuture}
import io.trane.ndbc.{DataSource, PreparedStatement, Row}

import scala.concurrent.duration.Duration

abstract class NdbcContext[I <: SqlIdiom, N <: NamingStrategy, P <: PreparedStatement, R <: Row](
  val idiom: I, val naming: N, val dataSource: DataSource[P, R]
)
  extends NdbcContextBase[I, N, P, R] {
  // TODO: This is not a TranslateContext. Why?
  // TODO: I removed some duplicated interfaces everywhere. I hope it doesn't mess with the macros

  override type Result[T] = Future[T]
  override type RunQueryResult[T] = List[T]
  override type RunQuerySingleResult[T] = T
  override type RunActionResult = Long
  override type RunActionReturningResult[T] = T
  override type RunBatchActionResult = List[Long]
  override type RunBatchActionReturningResult[T] = List[T]

  override implicit protected val resultEffect = new NdbcContextBase.ContextEffect[Future] {
    override def wrap[T](t: => T): Future[T] = Future(t)

    override def wrapAsync[T](f: (Complete[T]) => Unit): Future[T] = {
      val p = Promise[T]()
      f(p.complete)
      p.future
    }

    override def fmap[A, B](a: Future[A])(f: A => B): Future[B] = a.map(f)

    override def flatMap[A, B](a: Future[A])(f: A => Future[B]): Future[B] = a.flatMap(f)

    override def sequence[T](list: List[Future[T]]): Future[List[T]] = Future.sequence(list)

    override def runBlocking[T](eff: Future[T], timeout: Duration): T = Await.result(eff, timeout)
  }

  // Need explicit return-type annotations due to scala/bug#8356. Otherwise macro system will not understand Result[Long]=Long etc...
  override def executeAction[T](sql: String, prepare: Prepare = identityPrepare): Future[Long] =
    super.executeAction(sql, prepare)

  override def executeQuery[T](sql: String, prepare: Prepare = identityPrepare, extractor: Extractor[T] = identityExtractor): Future[List[T]] =
    super.executeQuery(sql, prepare, extractor)

  override def executeQuerySingle[T](sql: String, prepare: Prepare = identityPrepare, extractor: Extractor[T] = identityExtractor): Future[T] =
    super.executeQuerySingle(sql, prepare, extractor)

  override def executeActionReturning[O](sql: String, prepare: Prepare = identityPrepare, extractor: Extractor[O], returningBehavior: ReturnAction): Future[O] =
    super.executeActionReturning(sql, prepare, extractor, returningBehavior)

  override def executeBatchAction(groups: List[BatchGroup]): Future[List[Long]] =
    super.executeBatchAction(groups)

  override def executeBatchActionReturning[T](groups: List[BatchGroupReturning], extractor: Extractor[T]): Future[List[T]] =
    super.executeBatchActionReturning(groups, extractor)

  override def withDataSource[T](f: DataSource[P, R] => Future[T]): Future[T] = f(dataSource)
  /* TODO: I'm assuming that we don't need to bracket and close the dataSource like with JDBC
      because previously it wasn't done here either */

  // TODO: Is this applicable to NDBC? I could only find uses in the JDBC module
  def close(): Unit = dataSource.close()

  def transaction[T](f: => Future[T]): Future[T] = {
    implicit def javaSupplier[S](s: => S): Supplier[S] = new Supplier[S] {
      override def get = s
    }

    val javaFuturePool = FuturePool.apply(Executors.newCachedThreadPool())

    javaFuturePool.isolate(
      withDataSourceWrapped { ds =>
        ds.transactional(f.toJava).toScala
      }.toJava
    ).toScala
  }
}