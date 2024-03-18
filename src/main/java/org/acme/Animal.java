package org.acme;

import jakarta.persistence.Entity;
import io.quarkus.hibernate.orm.panache.PanacheEntity;
import io.quarkus.runtime.annotations.RegisterForReflection;

@RegisterForReflection
@Entity
public class Animal extends PanacheEntity {
    private Long id;
    private String name;
    private String type;

    public Animal() {
    }

    public Animal(Animal animal) {
        this.id = null;
        this.name = animal.name;
        this.type = "Cat";
    }

    public Animal(Long id, String name) {
        this.id = id;
        this.name = name;
    }

    public Animal(Long id, String name, String type) {
        this.id = id;
        this.name = name;
        this.type = type;
    }

    public Animal(String name) {
        this.id = null;
        this.name = name;
        this.type = "Cat";
    }

    public void setId(Long id) {
        this.id = id;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Long getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public String getType() {
        return type;
    }

    @Override
    public String toString() {
        return "Animal{" +
                "id='" + id + '\'' +
                ", name=" + name +
                '}';
    }
}