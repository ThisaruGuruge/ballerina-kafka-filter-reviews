import ballerina/io;
import wso2/kafka;
import ballerina/encoding;

string topicRejectedReviews = "rejected-reviews";

kafka:ConsumerConfig rejectedReviewConsumerConfigs = {
    bootstrapServers: "localhost:9092",
    groupId: "rejectedReviewConsumers",
    topics: [topicRejectedReviews]
};

listener kafka:SimpleConsumer rejectedReviewConsumer = new(rejectedReviewConsumerConfigs);

service handleRejectedReviewsService on rejectedReviewConsumer {
    resource function onMessage(kafka:SimpleConsumer simpleConsumer, kafka:ConsumerRecord[] records) {
        foreach var entry in records {
            io:println("Rejected Review Received");
            byte[] serializedMsg = entry.value;
            string msg = encoding:byteArrayToString(serializedMsg);

            io:println("[HandleFiltered]\t[INFO]\tAccepted Review: ");
            io:println("[HandleFiltered]\t[INFO]\t" + msg + "\n\n");

            kafka:TopicPartition topicPartition = {topic: entry.topic, partition: entry.partition};
            kafka:PartitionOffset offset = {partition: topicPartition, offset: entry.offset};
            var commitAcceptedResult = rejectedReviewConsumer->commitOffset([offset], duration = 10000);
        }
    }
}
