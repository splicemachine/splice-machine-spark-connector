/*
 * Copyright 2012 - 2020 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.splicemachine.sample;

import java.sql.Timestamp;
import java.util.*;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class KafkaTopicProducer {

    private String server = null;
    private long totalEvents = 1;
    private String topic = null;
    private KafkaProducer<String, String> producer = null;

    /**
     * Adds records to a Kafka queue
     *
     * @param args args[0] - Kafka Broker URL
     *             args[1] - Kafka Topic Name
     *             args[2] - Number of messages to add to the topic
     *             args[3] - Number of threads, each thread will send the number
     *                        of messages given in args[2]
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        KafkaTopicProducer kp = new KafkaTopicProducer();
        kp.server = args[0];
        kp.topic = args[1];
        kp.totalEvents = Long.parseLong(args[2]);
	kp.run(Long.parseLong(args[3]));
    }

    public void run(Long threadCount) {
	long n = 1024;
	//long idRange = n / threadCount;
	long idRange = (1+(threadCount/n))*n / threadCount;
	long a = 1;
	long b = idRange;
	KafkaProducer<String, String> kp = initProducer();
	for(long i=1; i <= threadCount; i++) {
		//KafkaProducer<String, String> kp = initProducer();
		new Thread(new MsgGenerator(a,idRange,kp)).start();
		a = b + 1;
		b = b + idRange;
	}
    }

    public KafkaProducer<String, String> initProducer() {
        //Define the properties for the Kafka Connection
	Properties props = new Properties();
	props.put("bootstrap.servers", server); //kafka server
	//props.put("acks", "all");
	//props.put("retries", 0);
        //props.put("batch.size", 16384); // default == 16384
        props.put("linger.ms", 10);
        //props.put("buffer.memory", 33554432); // default == 33554432
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        //Create a KafkaProducer using the Kafka Connection properties
        producer = new KafkaProducer<>(props);
	return producer;
    }

    private void close() {
	producer.close();
    }

	class Data{
		public String ID;
		public String PAYLOAD;
		public Timestamp TM;
	}

	public void t(String fMsg) {
		System.out.println( (new Date()).toString()+" "+fMsg );
	}

    /**
     * Sends messages to the Kafka queue.
     */
    class MsgGenerator implements Runnable {

     private long a0;
     private long idRange;
     private KafkaProducer<String, String> producer;

     public MsgGenerator(long a, long idRangeIn,
		KafkaProducer<String, String> kp)
     {
	a0 = a;
	idRange = idRangeIn;
	producer = kp;
     }

     private String payload() {
	StringBuilder sb = new StringBuilder();
	for(int i=1; i <= 20; i++) {
		sb.append( java.util.UUID.randomUUID().toString() );
	}
	return sb.toString();
     }

     public void run() {
        long nEvents = 0;

        //Loop through for the number of messages you want to put on the Queue
        for (nEvents = 0; nEvents < totalEvents; nEvents++) {
	    //t( "Make id" );
            String id = "A_" + ((nEvents % idRange) + a0);
	    //t( "Make Data" );
            Data d = new Data();
	    //t( "Assign id" );
            d.ID = id;
	    //t( "Assign payload" );
            d.PAYLOAD = payload(); // "PAYLOAD"; //getLocation();
	    //t( "Assign TM" );
            d.TM = new Timestamp(System.currentTimeMillis());

            //System.out.println( "timestamp: " + d.TM );

	    //t( "Convert to json" );
		//new com.google.gson.Gson().toJson(d)
            String json = new com.google.gson.GsonBuilder()
                 .setDateFormat("yyyy-MM-dd HH:mm:ss.S")
                 .create()
                 .toJson(d);

	    //t( "Create record" );
            ProducerRecord<String, String> rec =
		new ProducerRecord<String, String>(topic, id, json);
	    //t( "Send" );
            producer.send( rec );
        }
        //Flush and close the queue
        producer.flush();
        //producer.close();
        //display the number of messages that aw
        //System.out.println("messages pushed:" + nEvents);
     }
    }
}
