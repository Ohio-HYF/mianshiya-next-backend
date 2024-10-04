package com.hyf.mianshiya.service.impl;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.util.StrUtil;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.FieldSort;
import co.elastic.clients.elasticsearch._types.SortOrder;
import co.elastic.clients.elasticsearch._types.query_dsl.BoolQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.IdsQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch._types.query_dsl.QueryBuilders;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.elasticsearch.core.search.HitsMetadata;
import co.elastic.clients.util.ObjectBuilder;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.core.toolkit.Wrappers;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hyf.mianshiya.common.ErrorCode;
import com.hyf.mianshiya.constant.CommonConstant;
import com.hyf.mianshiya.constant.EsConstant;
import com.hyf.mianshiya.esdao.QuestionEsDTO;
import com.hyf.mianshiya.exception.ThrowUtils;
import com.hyf.mianshiya.mapper.QuestionMapper;
import com.hyf.mianshiya.model.dto.question.QuestionQueryRequest;
import com.hyf.mianshiya.model.entity.Question;
import com.hyf.mianshiya.model.entity.QuestionBankQuestion;
import com.hyf.mianshiya.model.entity.User;
import com.hyf.mianshiya.model.vo.QuestionVO;
import com.hyf.mianshiya.model.vo.UserVO;
import com.hyf.mianshiya.service.QuestionBankQuestionService;
import com.hyf.mianshiya.service.QuestionService;
import com.hyf.mianshiya.service.UserService;
import com.hyf.mianshiya.utils.SqlUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.ehcache.shadow.org.terracotta.context.query.QueryBuilder;
import org.springframework.context.annotation.Lazy;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.RequestBody;

import javax.annotation.Resource;
import javax.naming.directory.SearchResult;
import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * 题目服务实现
 *
 * @author <a href="https://github.com/Ohio-HYF">rainbow</a>
 */
@Service
@Slf4j
public class QuestionServiceImpl extends ServiceImpl<QuestionMapper, Question> implements QuestionService {

    @Resource
    private UserService userService;

    @Resource
    @Lazy
    private QuestionBankQuestionService questionBankQuestionService;

    @Resource
    private ElasticsearchClient elasticsearchClient;

    /**
     * 校验数据
     *
     * @param question
     * @param add      对创建的数据进行校验
     */
    @Override
    public void validQuestion(Question question, boolean add) {
        ThrowUtils.throwIf(question == null, ErrorCode.PARAMS_ERROR);
        // todo 从对象中取值
        String title = question.getTitle();
        String content = question.getContent();
        // 创建数据时，参数不能为空
        if (add) {
            // todo 补充校验规则
            ThrowUtils.throwIf(StringUtils.isBlank(title), ErrorCode.PARAMS_ERROR);
        }
        // 修改数据时，有参数则校验
        // todo 补充校验规则
        if (StringUtils.isNotBlank(title)) {
            ThrowUtils.throwIf(title.length() > 80, ErrorCode.PARAMS_ERROR, "标题过长");
            ThrowUtils.throwIf(content.length() > 10240, ErrorCode.PARAMS_ERROR, "内容过长");
        }
    }

    /**
     * 获取查询条件
     *
     * @param questionQueryRequest
     * @return
     */
    @Override
    public QueryWrapper<Question> getQueryWrapper(QuestionQueryRequest questionQueryRequest) {
        QueryWrapper<Question> queryWrapper = new QueryWrapper<>();
        if (questionQueryRequest == null) {
            return queryWrapper;
        }
        // todo 从对象中取值
        Long id = questionQueryRequest.getId();
        Long notId = questionQueryRequest.getNotId();
        String title = questionQueryRequest.getTitle();
        String content = questionQueryRequest.getContent();
        String sortField = questionQueryRequest.getSortField();
        String sortOrder = questionQueryRequest.getSortOrder();
        List<String> tagList = questionQueryRequest.getTags();
        Long userId = questionQueryRequest.getUserId();
        // todo 补充需要的查询条件
        // 模糊查询
        queryWrapper.like(StringUtils.isNotBlank(title), "title", title);
        queryWrapper.like(StringUtils.isNotBlank(content), "content", content);
        // JSON 数组查询
        if (CollUtil.isNotEmpty(tagList)) {
            for (String tag : tagList) {
                queryWrapper.like("tags", "\"" + tag + "\"");
            }
        }
        // 精确查询
        queryWrapper.ne(ObjectUtils.isNotEmpty(notId), "id", notId);
        queryWrapper.eq(ObjectUtils.isNotEmpty(id), "id", id);
        queryWrapper.eq(ObjectUtils.isNotEmpty(userId), "userId", userId);
        // 排序规则
        queryWrapper.orderBy(SqlUtils.validSortField(sortField),
                sortOrder.equals(CommonConstant.SORT_ORDER_ASC),
                sortField);
        return queryWrapper;
    }

    /**
     * 获取题目封装
     *
     * @param question
     * @param request
     * @return
     */
    @Override
    public QuestionVO getQuestionVO(Question question, HttpServletRequest request) {
        // 对象转封装类
        QuestionVO questionVO = QuestionVO.objToVo(question);

        // todo 可以根据需要为封装对象补充值，不需要的内容可以删除
        // region 可选
        // 1. 关联查询用户信息
        Long userId = question.getUserId();
        User user = null;
        if (userId != null && userId > 0) {
            user = userService.getById(userId);
        }
        UserVO userVO = userService.getUserVO(user);
        questionVO.setUser(userVO);
        // endregion

        return questionVO;
    }

    /**
     * 分页获取题目封装
     *
     * @param questionPage
     * @param request
     * @return
     */
    @Override
    public Page<QuestionVO> getQuestionVOPage(Page<Question> questionPage, HttpServletRequest request) {
        List<Question> questionList = questionPage.getRecords();
        Page<QuestionVO> questionVOPage = new Page<>(questionPage.getCurrent(), questionPage.getSize(), questionPage.getTotal());
        if (CollUtil.isEmpty(questionList)) {
            return questionVOPage;
        }
        // 对象列表 => 封装对象列表
        List<QuestionVO> questionVOList = questionList.stream().map(question -> {
            return QuestionVO.objToVo(question);
        }).collect(Collectors.toList());

        // todo 可以根据需要为封装对象补充值，不需要的内容可以删除
        // region 可选
        // 1. 关联查询用户信息
        Set<Long> userIdSet = questionList.stream().map(Question::getUserId).collect(Collectors.toSet());
        Map<Long, List<User>> userIdUserListMap = userService.listByIds(userIdSet).stream()
                .collect(Collectors.groupingBy(User::getId));
        // 填充信息
        questionVOList.forEach(questionVO -> {
            Long userId = questionVO.getUserId();
            User user = null;
            if (userIdUserListMap.containsKey(userId)) {
                user = userIdUserListMap.get(userId).get(0);
            }
            questionVO.setUser(userService.getUserVO(user));
        });
        // endregion

        questionVOPage.setRecords(questionVOList);
        return questionVOPage;
    }

    /**
     * 分页获取题目列表（仅管理员可用）
     *
     * @param questionQueryRequest
     * @return
     */
    public Page<Question> listQuestionByPage(QuestionQueryRequest questionQueryRequest) {
        long current = questionQueryRequest.getCurrent();
        long size = questionQueryRequest.getPageSize();

        // 题目表的查询条件
        QueryWrapper<Question> queryWrapper = this.getQueryWrapper(questionQueryRequest);

        Long questionBankId = questionQueryRequest.getQuestionBankId();
        if (questionBankId != null) { // 如果传入题库id
            // 查询题库关联的题目
            LambdaQueryWrapper<QuestionBankQuestion> lambdaQueryWrapper = Wrappers.lambdaQuery(QuestionBankQuestion.class)
                    .select(QuestionBankQuestion::getQuestionId)
                    .eq(QuestionBankQuestion::getQuestionBankId, questionBankId);
            List<QuestionBankQuestion> questions = questionBankQuestionService.list(lambdaQueryWrapper);
            if (CollUtil.isNotEmpty(questions)) {
                Set<Long> questionIds = questions.stream().map(QuestionBankQuestion::getQuestionId).collect(Collectors.toSet());
                queryWrapper.in("id", questionIds);
            }
        }

        // 查询题目表
        return this.page(new Page<>(current, size), queryWrapper);
    }

    /**
     * 从ES中查询题目
     *
     * @return
     * @Param questionQueryRequest
     */
    @Override
    public Page<Question> searchFromEs(QuestionQueryRequest questionQueryRequest) {
        // 获取参数
        Long id = questionQueryRequest.getId();
        Long notId = questionQueryRequest.getNotId();
        String searchText = questionQueryRequest.getSearchText();
        List<String> tags = questionQueryRequest.getTags();
        Long questionBankId = questionQueryRequest.getQuestionBankId();
        Long userId = questionQueryRequest.getUserId();
        // 注意，ES 的起始页为 0
        int current = questionQueryRequest.getCurrent() - 1;
        int pageSize = questionQueryRequest.getPageSize();
        String sortField = questionQueryRequest.getSortField();
        String sortOrder = questionQueryRequest.getSortOrder();


        // 构造查询请求
        SearchRequest.Builder builder = new SearchRequest.Builder();
        builder.index(EsConstant.QUESTION_INDEX);
        List<Query> mustQueries = new ArrayList<>();
        List<Query> mustNotQueries = new ArrayList<>();
        List<Query> shouldQueries = new ArrayList<>();
        List<Query> filterQueries = new ArrayList<>();

        // 精确搜索
        if (id != null) {
            mustQueries.add(QueryBuilders
                    .ids()
                    .values(id.toString())
                    .build()._toQuery());
        }
        if (notId != null) {
            mustNotQueries.add(QueryBuilders
                    .term()
                    .field("id")
                    .value(notId.toString())
                    .build()._toQuery());
        }
        if (userId != null) {
            filterQueries.add(QueryBuilders
                    .term()
                    .field("userId")
                    .value(userId.toString())
                    .build()._toQuery());
        }
        if (questionBankId != null) {
            filterQueries.add(QueryBuilders
                    .term()
                    .field("questionBankId")
                    .value(questionBankId.toString())
                    .build()._toQuery());
        }
        if (CollUtil.isNotEmpty(tags)) {
            for (String tag : tags)
                filterQueries.add(QueryBuilders
                        .term()
                        .field("tags")
                        .value(tag)
                        .build()._toQuery());
        }
        // 模糊搜索
        if (StrUtil.isNotBlank(searchText)) {
            shouldQueries.add(
                    QueryBuilders.bool()
                            .should(QueryBuilders.match().field("title").query(searchText).build()._toQuery())
                            .should(QueryBuilders.match().field("content").query(searchText).build()._toQuery())
                            .should(QueryBuilders.match().field("answer").query(searchText).build()._toQuery())
                            .build()._toQuery());
        }
        builder.query(q -> q
                .bool(b -> b
                        .must(mustQueries)
                        .mustNot(mustNotQueries)
                        .filter(filterQueries)
                        .should(shouldQueries)));
        // 添加排序条件
        if (StrUtil.isNotBlank(sortField)) {
            builder.sort(s -> s
                    .field(f -> f
                            .field(sortField)
                            .order(CommonConstant.SORT_ORDER_ASC.equals(sortOrder) ? SortOrder.Asc : SortOrder.Desc)));
        }
        // 添加分页参数
        builder.from(current * pageSize);
        builder.size(pageSize);

        Page<Question> page = new Page<>();
        try {
            SearchRequest searchRequest = builder.build();

            SearchResponse<QuestionEsDTO> response = elasticsearchClient.search(searchRequest, QuestionEsDTO.class);
            List<Hit<QuestionEsDTO>> hits = response.hits().hits();

            page.setTotal(response.hits().total().value());
            List<Question> resourceList = new ArrayList<>();
            for (Hit<QuestionEsDTO> hit : hits) {
                resourceList.add(QuestionEsDTO.dtoToObj(hit.source()));
            }
            page.setRecords(resourceList);
        } catch (IOException | NullPointerException e) {
            log.error("ES查询异常", e);
            return null;
        }
        return page;
    }
}
