package com.valhalla.runnable;

import com.valhalla.ServerApplication;
import com.valhalla.holder.base.parse.MessageParse2Sql;
import com.valhalla.mapper.TopicTableMapper;
import com.valhalla.thread.NamedThreadFactory;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author : LynX
 * @create 2024/1/11 14:31
 */
@Data
@Slf4j
public class MessageParseWriteRunnable implements Runnable {
    private final JdbcTemplate jdbcTemplate = ServerApplication.getContext().getBean(JdbcTemplate.class);
    private final AtomicBoolean state = new AtomicBoolean(false);
    private final AtomicBoolean writing = new AtomicBoolean(false);
    private final TopicTableMapper topicTableMapper;

    private final List<String> copyList = Collections.synchronizedList(new ArrayList<>());

    ThreadPoolExecutor jdbcExecutor = new ThreadPoolExecutor(
            1,
            4,
            0L,
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(),
            new NamedThreadFactory(MessageParseWriteRunnable.class.getSimpleName())
    );

    public MessageParseWriteRunnable(TopicTableMapper topicTableMapper) {
        this.topicTableMapper = topicTableMapper;
    }

    @Override
    public void run() {
        state.set(true);
        while (state.get()) {
            if (writing.get()) {
                synchronized (copyList) {
                    parseWrite();
                    copyList.clear();
                    writing.set(false);
                }
            } else {
                log.debug("MessageParseWriteRunnable is not ready");
            }
            try {
                // Sleep for a short time to avoid busy waiting
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    private void parseWrite() {
        MessageParse2Sql messageParse2Sql = topicTableMapper.getMessageParse2Sql();
        String sql = null;
        try {
            sql = messageParse2Sql.parseToSql(topicTableMapper.getTable(), topicTableMapper.getColumns(), copyList);
        } catch (Exception e) {
            log.error("parseToSql error {} - {} - {}", e.getMessage(), topicTableMapper.getTable(), copyList);
        }
        if (sql != null) {
            jdbcExecutor.execute(new JdbcRunnable(sql));
        }
    }

    class JdbcRunnable implements Runnable {
        private final String sql;

        public JdbcRunnable(String sql) {
            this.sql = sql;
        }

        @Override
        public void run() {
            jdbcTemplate.update(sql);
        }
    }
}
