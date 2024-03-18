package org.acme;

public class Mammal extends Animal {
    public Integer numberNipples;

    public Mammal(Animal animal) {
        super(animal);
        this.numberNipples = 4;
    }

    public Integer getNumberNipples() {
        return numberNipples;
    }

    public void setNumberNipples(Integer numberNipples) {
        this.numberNipples = numberNipples;
    }

}
