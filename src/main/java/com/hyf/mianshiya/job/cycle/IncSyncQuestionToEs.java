package com.hyf.mianshiya.job.cycle;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import com.hyf.mianshiya.constant.EsConstant;
import com.hyf.mianshiya.esdao.QuestionEsDTO;
import com.hyf.mianshiya.mapper.QuestionMapper;
import com.hyf.mianshiya.model.entity.Question;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.Date;
import java.util.List;

@Component
@Slf4j
public class IncSyncQuestionToEs {
    @Resource
    private QuestionMapper questionMapper;
    @Resource
    private ElasticsearchClient elasticsearchClient;

    /**
     * 每分钟执行一次
     */
    @Scheduled(fixedRate = 60 * 1000)
    public void run() throws IOException {
        log.info("开始执行增量同步题目到ES");
        // 查询五分钟内更新的数据（包含逻辑删除）
        Date date = new Date(System.currentTimeMillis() - 5 * 60 * 1000);
        List<Question> questions = questionMapper.listQuestionWithDelete(date);
        if (questions == null || questions.isEmpty()) {
            log.info("没有需要同步的数据");
            return;
        }

        // 同步到ES
        List<QuestionEsDTO> questionEsDTOS = questions.stream()
                .map(QuestionEsDTO::objToDto)
                .toList();

        // 分批同步到ES中
        int batchSize = 500;
        int batchCount = questionEsDTOS.size() / batchSize;
        if (questionEsDTOS.size() % batchSize != 0) {
            batchCount++;
        }
        for (int i = 0; i < batchCount; i++) {
            BulkRequest.Builder br = new BulkRequest.Builder();
            for (int j = i * batchSize; j < (i + 1) * batchSize && j < questionEsDTOS.size(); j++) {
                QuestionEsDTO question = questionEsDTOS.get(j);
                br.operations(op -> op
                        .index(idx -> idx
                                .index(EsConstant.QUESTION_INDEX)
                                .id(question.getId().toString())
                                .document(question)
                        )
                );
            }
            BulkResponse result = elasticsearchClient.bulk(br.build());
            // Log errors, if any
            if (result.errors()) {
                log.error("Bulk had errors");
                for (BulkResponseItem item : result.items()) {
                    if (item.error() != null) {
                        log.error(item.error().reason());
                        // todo: 异常处理
                    }
                }
            }
            log.info(String.format("数据同步进度: %d\\%d", i+1, batchCount));
        }

        log.info("数据同步完成");
    }

}
