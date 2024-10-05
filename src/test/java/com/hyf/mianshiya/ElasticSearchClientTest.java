package com.hyf.mianshiya;

import cn.hutool.core.date.DateUtil;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.Refresh;
import co.elastic.clients.elasticsearch._types.query_dsl.MatchQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch._types.query_dsl.RangeQuery;
import co.elastic.clients.elasticsearch.core.*;
import co.elastic.clients.elasticsearch.core.search.Hit;
import io.swagger.models.auth.In;
import lombok.Data;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
public class ElasticSearchClientTest {

    @Autowired
    private ElasticsearchClient elasticsearchClient;

    private final String INDEX_NAME = "product_index";

    @Data
    private static class Product {
        private String sku;
        private String title;
        private String description;
        private Integer price;
        private Date date;
    }

    /**
     * 向Index中插入Docdument，通过传入IndexRequest的实例化对象
     */
    @Test
    public void testIndexDocumentByIndexRequest() {
        try {
            Product product = new Product();
            product.setSku("1");
            product.setTitle("bike");
            product.setDescription("A red bike");
            product.setPrice(200);
            product.setDate(DateUtil.parse("2024-10-03 21:06:30", "yyyy-MM-dd HH:mm:ss"));

            IndexRequest<Product> request = IndexRequest.of(i -> i
                    .index(INDEX_NAME)
                    .id(product.getSku())
                    .document(product));

            IndexResponse response = elasticsearchClient.index(request);
            assertThat(response).isNotNull();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 向Index中插入Docdument，直接使用函数式编程创建Request
     */
    @Test
    public void testIndexByFunctional() {
        try {
            Product product = new Product();
            product.setSku("2");
            product.setTitle("bike");
            product.setDescription("A blue bike");
            product.setPrice(200);
            product.setDate(DateUtil.parse("2024-10-03 21:06:30", "yyyy-MM-dd HH:mm:ss"));

            IndexResponse response = elasticsearchClient.index(i -> i
                    .index(INDEX_NAME)
                    .id(product.getSku())
                    .document(product));
            assertThat(response).isNotNull();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取Index的全部Document
     */
    @Test
    public void testAllGet() {
        try {
            SearchRequest searchRequest = SearchRequest.of(s -> s
                    .index(INDEX_NAME));
            SearchResponse<Product> response = elasticsearchClient.search(searchRequest, Product.class);
            List<Hit<Product>> hits = response.hits().hits();
            for (Hit<Product> hit : hits) {
                System.out.println(hit.source());
            }
            assertThat(response).isNotNull();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 查询Document
     */
    @Test
    public void testSearch() {
        try {
            Query byDescription = MatchQuery.of(m -> m
                    .field("description")
                    .query("red")
            )._toQuery();

            // Combine name and price queries to search the product index
            SearchResponse<Product> response = elasticsearchClient.search(s -> s
                            .index(INDEX_NAME)
                            .query(q -> q
                                    .bool(b -> b
                                            .filter(byDescription)
                                    )
                            ),
                    Product.class
            );
            List<Hit<Product>> hits = response.hits().hits();
            for (Hit<Product> hit : hits)
                System.out.println(hit.source());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
