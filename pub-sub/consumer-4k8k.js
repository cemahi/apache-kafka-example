const { Kafka } = require("kafkajs");

createConsumer();

async function createConsumer(){
    try {
        const kafka  = new Kafka({
            clientId : "kafka_pub_sub_client",
            brokers : ["192.168.1.116:9092"]
        })
    
        const consumer = kafka.consumer({
            groupId : "hd_4k_8k_encoder_consumer_group"
        });
        console.log("Consumer'a baglaniliyor...")
        await consumer.connect();
        console.log("Consumer'a baglanti basarili...")

        //Consumer Subscribe ...
        await consumer.subscribe({
            topic : "raw_video_topic",
            fromBeginning : true
        })

        await consumer.run({
            eachMessage : async result => {
                console.log(`Gelen Mesaj ${result.message.value}_4k_8k_encoder`)
            }
        })
    } catch (error) {
        console.log("Bir hata olustu", error)
    }
}