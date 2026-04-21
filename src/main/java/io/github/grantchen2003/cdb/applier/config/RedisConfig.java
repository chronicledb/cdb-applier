package io.github.grantchen2003.cdb.applier.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.StringRedisSerializer;

@Configuration
public class RedisConfig {

    private static final Logger log = LoggerFactory.getLogger(RedisConfig.class);

    @Bean
    public RedisTemplate<String, String> redisTemplate(RedisConnectionFactory factory) {
        final RedisTemplate<String, String> template = new RedisTemplate<>();
        template.setConnectionFactory(factory);
        template.setKeySerializer(new StringRedisSerializer());
        template.setHashKeySerializer(new StringRedisSerializer());
        template.setHashValueSerializer(new StringRedisSerializer());
        return template;
    }

    @Bean
    public ApplicationRunner redisConnectionCheck(RedisTemplate<String, String> redisTemplate) {
        return args -> {
            try {
                final RedisConnectionFactory factory = redisTemplate.getConnectionFactory();
                if (factory == null) {
                    log.error("Redis connection FAILED: connection factory is null");
                    return;
                }
                final String pong = factory.getConnection().ping();
                log.info("Redis connection OK: {}", pong);
            } catch (Exception e) {
                log.error("Redis connection FAILED", e);
            }
        };
    }
}
