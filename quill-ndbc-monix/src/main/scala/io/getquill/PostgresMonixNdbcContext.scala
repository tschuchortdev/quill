package io.getquill

import java.io.Closeable

import com.typesafe.config.Config
import io.getquill.context.monix.Runner
import io.getquill.context.ndbc.NdbcContextConfig
import io.getquill.util.LoadConfig
import javax.sql.DataSource

class PostgresMonixNdbcContext[N <: NamingStrategy](
  val naming:     N,
  val dataSource: DataSource with Closeable,
  runner:         Runner
) extends MonixNdbcContext[PostgresDialect, N](dataSource, runner)
  with PostgresNdbcContextBase[N] {

  def this(naming: N, config: NdbcContextConfig, runner: Runner) = this(naming, config.dataSource, runner)
  def this(naming: N, config: Config, runner: Runner) = this(naming, NdbcContextConfig(config), runner)
  def this(naming: N, configPrefix: String, runner: Runner) = this(naming, LoadConfig(configPrefix), runner)
  def this(naming: N, configPrefix: String) = this(naming, LoadConfig(configPrefix), Runner.default)
}