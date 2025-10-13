package org.example;

import java.util.Objects;

public class Triplet {
    private String subject;
    private String predicate;
    private String object;

    public Triplet() {}

    public Triplet(String subject, String predicate, String object) {
        this.subject = subject;
        this.predicate = predicate;
        this.object = object;
    }

    // Constructor for dictionary
    public Triplet(int subject, int predicate, int object) {
        this.subject = String.valueOf(subject);
        this.predicate = String.valueOf(predicate);
        this.object = String.valueOf(object);
    }

    public String getSubject() {
        return subject;
    }

    public void setSubject(String subject) {
        this.subject = subject;
    }

    public String getPredicate() {
        return predicate;
    }

    public void setPredicate(String predicate) {
        this.predicate = predicate;
    }

    public String getObject() {
        return object;
    }

    public void setObject(String object) {
        this.object = object;
    }

    public void clear() {
        this.subject = null;
        this.predicate = null;
        this.object = null;
    }

    @Override
    public String toString() {
        return "Triplet{" +
                "subject='" + subject + '\'' +
                ", predicate='" + predicate + '\'' +
                ", object='" + object + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Triplet)) return false;
        Triplet t = (Triplet) o;
        return subject.equals(t.subject)
                && predicate.equals(t.predicate)
                && object.equals(t.object);
    }

    @Override
    public int hashCode() {
        return Objects.hash(subject, predicate, object);
    }


}
