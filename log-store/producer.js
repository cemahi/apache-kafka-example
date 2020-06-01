const { Kafka } = require("kafkajs");
const log_data = require("./system-logs.json");
createProcuder();

async function createProcuder(){
    try {
        const kafka  = new Kafka({
            clientId : "kafka_log_store_client",
            brokers : ["192.168.1.116:9092"]
        })
    
        const producer = kafka.producer();
        console.log("Producer'a baglaniliyor...")
        await producer.connect();
        console.log("Producer'a baglanti basarili...")

        let messages = log_data.map(item => {
            return {
                value: JSON.stringify(item),
                partition: item.type == "system" ? 0 : 1
            }
        })

        const message_result = await producer.send({
            topic : "logStoreTopic",
            messages : messages
        });

        console.log("Gonderim islemi basarilidir", JSON.stringify(message_result));
        await producer.disconnect();
    } catch (error) {
        console.log("Bir hata olustu", error)
    }finally{
        process.exit(0);
    }
}