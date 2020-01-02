package io.getquill.context.ndbc

import java.util

import cats._
import cats.effect.Async
import cats.implicits._
import io.getquill._
import io.getquill.context.ContextEffect
import io.getquill.context.ndbc.NdbcContextBase.ContextEffect
import io.getquill.context.sql.SqlContext
import io.getquill.context.sql.idiom.SqlIdiom
import io.getquill.util.ContextLogger
import io.trane.future.scala.{Await, Future, toScalaFuture}
import io.trane.ndbc.{DataSource, PreparedStatement, Row}

import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}

object NdbcContextBase {
  trait ContextEffect[F[_]] extends context.ContextEffect[F] {
    final type Complete[T] = (Try[T] => Unit)
    def wrapAsync[T](f: Complete[T] => Unit): F[T]

    def flatMap[A, B](a: F[A])(f: A => F[B]): F[B]
    def traverse[A, B](list: List[A])(f: A => F[B]) = sequence(list.map(f))

    def runBlocking[T](eff: F[T], timeout: Duration): T
  }
}

trait NdbcContextBase[Idiom <: SqlIdiom, Naming <: NamingStrategy, P <: PreparedStatement, R <: Row]
  extends SqlContext[Idiom, Naming] {

  // TODO: Changed to ContextLogger. Was regular Logger previously.
  private[getquill] val logger = ContextLogger(classOf[NdbcContext[_, _, _, _]])

  // TODO: Why are PrepareRow and ResultRow now type parameters instead of member types?
  final override type PrepareRow = P
  final override type ResultRow = R

  protected implicit val resultEffect: NdbcContextBase.ContextEffect[Result]
  import resultEffect._


  // TODO: Why not just make the dataSource a member variable?
  protected def withDataSource[T](f: DataSource[P, R] => Result[T]): Result[T]

  final protected def withDataSourceWrapped[T](f: DataSource[P, R] => Future[T]): Result[T] =
    withDataSource { ds =>
      wrapAsync { complete: =>
        f(ds).onComplete(complete)
      }
    }

  protected def createPreparedStatement(sql: String): P

  // TODO: Can this be final?
  protected def expandAction(sql: String, returningAction: ReturnAction) = sql

  def executeQuery[T](sql: String, prepare: Prepare = identityPrepare, extractor: R => T = identity[R] _): Result[List[T]] = {
    withDataSourceWrapped { ds =>
      val (params, ps) = prepare(createPreparedStatement(sql))
      logger.logQuery(sql, params)

      ds.query(ps).toScala.map { rs =>
        extractResult(rs.iterator, extractor)
      }
    }
  }

  def executeQuerySingle[T](sql: String, prepare: Prepare = identityPrepare, extractor: R => T = identity[R] _): Result[T] =
    fmap(executeQuery(sql, prepare, extractor))(handleSingleResult)

  def executeAction[T](sql: String, prepare: Prepare = identityPrepare): Result[Long] = {
    withDataSourceWrapped { ds =>
      val (params, ps) = prepare(createPreparedStatement(sql))
      logger.logQuery(sql, params)
      ds.execute(ps).toScala.map(_.longValue)
    }
  }

  def executeActionReturning[O](sql: String, prepare: Prepare = identityPrepare, extractor: R => O, returningAction: ReturnAction): Result[O] = {
    val expanded = expandAction(sql, returningAction)
    executeQuerySingle(expanded, prepare, extractor)
  }

  def executeBatchAction(groups: List[BatchGroup]): Result[List[Long]] =
    fmap(
      traverse(groups) { case BatchGroup(sql, prepares) =>
          prepares.foldLeft(wrap(ArrayBuffer.empty[Long])) { (acc, prepare) =>
            flatMap(acc) { array =>
              fmap(executeAction(sql, prepare))(array :+ _)
            }
          }
      }
    )(_.flatten)

  // TODO: Should this be blocking? Previously it was just a Future wrapped in a Try, which makes no sense
  def probe(sql: String): Try[_] =
    Try(runBlocking(withDataSourceWrapped(_.query(sql).toScala), Duration.Inf))

  def executeBatchActionReturning[T](groups: List[BatchGroupReturning], extractor: R => T): Result[List[T]] =
    fmap(
      traverse(groups) { case BatchGroupReturning(sql, column, prepare) =>
          prepare.foldLeft(wrap(ArrayBuffer.empty[T])) { (acc, prepare) =>
            flatMap(acc) { array =>
              fmap(executeActionReturning(sql, prepare, extractor, column))(array :+ _)
            }
          }
      }
    )(_.flatten)

  @tailrec
  private def extractResult[T](rs: util.Iterator[R], extractor: R => T, acc: List[T] = Nil): List[T] =
    if (rs.hasNext)
      extractResult(rs, extractor, extractor(rs.next()) :: acc)
    else
      acc.reverse
}