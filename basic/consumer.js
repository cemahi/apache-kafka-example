const { Kafka } = require("kafkajs");

const topic_name = process.argv[2] || "logs2"

createConsumer();

async function createConsumer(){
    try {
        const kafka  = new Kafka({
            clientId : "kafka_ornek_1",
            brokers : ["192.168.1.116:9092"]
        })
    
        const consumer = kafka.consumer({
            groupId : "ornek_1_cg_1"
        });
        console.log("Consumer'a baglaniliyor...")
        await consumer.connect();
        console.log("Consumer'a baglanti basarili...")

        //Consumer Subscribe ...
        await consumer.subscribe({
            topic : topic_name,
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