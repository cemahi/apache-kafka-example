const { Kafka } = require("kafkajs");
createProcuder();

async function createProcuder(){
    try {
        const kafka  = new Kafka({
            clientId : "kafka_pub_sub_client",
            brokers : ["192.168.1.116:9092"]
        })
    
        const producer = kafka.producer();
        console.log("Producer'a baglaniliyor...")
        await producer.connect();
        console.log("Producer'a baglanti basarili...")

        const message_result = await producer.send({
            topic : "raw_video_topic",
            messages : [
                {
                    value: "Yeni video icerigi",
                    partition: 0
                }
            ]
        });

        console.log("Gonderim islemi basarilidir", JSON.stringify(message_result));
        await producer.disconnect();
    } catch (error) {
        console.log("Bir hata olustu", error)
    }finally{
        process.exit(0);
    }
}