const { Kafka } = require("kafkajs");

createConsumer();

async function createConsumer(){
    try {
        const kafka  = new Kafka({
            clientId : "kafka_log_store_client",
            brokers : ["192.168.1.116:9092"]
        })
    
        const consumer = kafka.consumer({
            groupId : "log_store_consumer_group"
        });
        console.log("Consumer'a baglaniliyor...")
        await consumer.connect();
        console.log("Consumer'a baglanti basarili...")

        //Consumer Subscribe ...
        await consumer.subscribe({
            topic : "logStoreTopic",
            fromBeginning : true
        })

        await consumer.run({
            eachMessage : async result => {
                console.log(`Gelen Mesaj ${result.message.value} : Partition : => ${result.partition}`)
            }
        })
    } catch (error) {
        console.log("Bir hata olustu", error)
    }
}