package com.learnkafka.domain;

import java.io.Serializable;

public class Item implements Serializable{

    private static final long serialVersionUID = 1969906832571875737L;
    private Integer id;
    private String itemName;
    private Double price;
    public Item(){

    }

    public Item(Integer id, String itemName, Double price) {
        this.id = id;
        this.itemName = itemName;
        this.price = price;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getItemName() {
        return itemName;
    }

    public void setItemName(String itemName) {
        this.itemName = itemName;
    }

    public Double getPrice() {
        return price;
    }

    public void setPrice(Double price) {
        this.price = price;
    }

    @Override
    public String toString() {
        return "Item{" +
                "id=" + id +
                ", itemName='" + itemName + '\'' +
                ", price=" + price +
                '}';
    }
}
