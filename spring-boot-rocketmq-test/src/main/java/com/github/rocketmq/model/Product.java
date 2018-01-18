package com.github.rocketmq.model;

import lombok.Data;
import lombok.ToString;

import java.math.BigDecimal;

/**
 * @author Stephen
 * @since 2018-01-18
 */
@Data
@ToString
public class Product {

    private String name;
    private BigDecimal price;
    private String origin;

    private String tags;
}
