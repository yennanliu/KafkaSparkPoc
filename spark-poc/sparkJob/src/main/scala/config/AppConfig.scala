package config

import com.typesafe.config.{Config, ConfigFactory}

object AppConfig {

  private val cfg = ConfigFactory.load()

  val env: String = if (cfg.hasPath("env")) {
    cfg.getString("env") } else "default"

  val config: Config = cfg
    .getConfig(env)
    .withFallback(cfg.getConfig("default"))

}
