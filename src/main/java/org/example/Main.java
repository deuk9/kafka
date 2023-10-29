package org.example;

public class Main {
    public static void main(String[] args) {
        SampleProducer producer = new SampleProducer();
        producer.init();
        producer.send();
        producer.sendHasKey();
        producer.sendHasKeyAndPartition();
        producer.sendThenConfirm();
        producer.stop();
    }
}