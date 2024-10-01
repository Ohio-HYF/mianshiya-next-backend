package com.hyf.mianshiya.model.dto.questionBank;

import com.hyf.mianshiya.common.PageRequest;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.io.Serializable;
import java.util.List;

/**
 * 查询题库请求
 *
 * @author <a href="https://github.com/Ohio-HYF">rainbow</a>
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class QuestionBankQueryRequest extends PageRequest implements Serializable {

    /**
     * id
     */
    private Long id;

    /**
     * id
     */
    private Long notId;

    /**
     * 搜索词
     */
    private String searchText;

    /**
     * 描述
     */
    private String description;

    /**
     * 标题
     */
    private String title;

    /**
     * 创建用户 id
     */
    private Long userId;

    private static final long serialVersionUID = 1L;
}