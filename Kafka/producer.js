const { Kafka } = require("kafkajs");

const topic_name = process.argv[2] || "logs2"
const partition = process.argv[3] || 0
createProcuder();

async function createProcuder(){
    try {
        const kafka  = new Kafka({
            clientId : "kafka_ornek_1",
            brokers : ["192.168.1.116:9092"]
        })
    
        const producer = kafka.producer();
        console.log("Producer'a baglaniliyor...")
        await producer.connect();
        console.log("Producer'a baglanti basarili...")

        const message_result = await producer.send({
            topic : topic_name,
            messages : [
                {
                    value: "Bu bir test log mesajidir...",
                    partition: partition
                }
            ]
        })
        console.log("Gonderim islemi basarilidir", JSON.stringify(message_result));
        await producer.disconnect();
    } catch (error) {
        console.log("Bir hata olustu", error)
    }finally{
        process.exit(0);
    }
}