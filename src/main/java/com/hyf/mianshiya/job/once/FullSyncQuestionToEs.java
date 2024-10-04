package com.hyf.mianshiya.job.once;

import cn.hutool.core.util.StrUtil;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import com.hyf.mianshiya.constant.EsConstant;
import com.hyf.mianshiya.esdao.QuestionEsDTO;
import com.hyf.mianshiya.mapper.QuestionMapper;
import com.hyf.mianshiya.model.entity.Question;
import com.hyf.mianshiya.service.QuestionService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

@Component
@Slf4j
public class FullSyncQuestionToEs implements CommandLineRunner {
    @Resource
    private QuestionService questionService;
    @Resource
    private ElasticsearchClient elasticsearchClient;

    @Override
    public void run(String... args) throws IOException {
        log.info("开始同步数据到ES中");
        // 从数据库中查询题目数据，数据量不大，直接全量查询
        List<Question> questions = questionService.list();
        if (questions == null || questions.isEmpty()) {
            return;
        }
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
