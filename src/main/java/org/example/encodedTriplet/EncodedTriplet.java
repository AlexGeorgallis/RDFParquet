package org.example.encodedTriplet;

import java.util.Objects;

public class EncodedTriplet {

    private int subject;
    private int predicate;
    private int object;

    public EncodedTriplet(int subject, int predicate, int object) {
        this.subject = subject;
        this.predicate = predicate;
        this.object = object;
    }


    public int getSubject() {
        return subject;
    }

    public int getPredicate() {
        return predicate;
    }

    public int getObject() {
        return object;
    }


    public void setSubject(int subject) {
        this.subject = subject;
    }

    public void setPredicate(int predicate) {
        this.predicate = predicate;
    }

    public void setObject(int object) {
        this.object = object;
    }

    @Override
    public String toString() {
        return subject + ", " + predicate + ", " + object;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EncodedTriplet that = (EncodedTriplet) o;
        return subject == that.subject &&
                predicate == that.predicate &&
                object == that.object;
    }
    @Override
    public int hashCode() {
        return Objects.hash(subject, predicate, object);
    }

}
