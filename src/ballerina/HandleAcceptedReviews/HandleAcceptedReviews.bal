import ballerina/io;
import wso2/kafka;
import ballerina/encoding;

string topicAcceptedReviews = "accepted-reviews";

kafka:ConsumerConfig acceptedReviewsConsumerConfigs = {
    bootstrapServers: "localhost:9092",
    groupId: "AcceptedReviewConsumers",
    topics: [topicAcceptedReviews]
};

listener kafka:SimpleConsumer acceptedReviewConsumer = new(acceptedReviewsConsumerConfigs);

service handleAcceptedReviewsService on acceptedReviewConsumer {
    resource function onMessage(kafka:SimpleConsumer simpleConsumer, kafka:ConsumerRecord[] records) {
        foreach var entry in records {
            io:println("Accepted Review Received");
            byte[] serializedMsg = entry.value;
            string msg = encoding:byteArrayToString(serializedMsg);

            io:println("[HandleFiltered]\t[INFO]\tAccepted Review: ");
            io:println("[HandleFiltered]\t[INFO]\t" + msg + "\n\n");

            kafka:TopicPartition topicPartition = {topic: entry.topic, partition: entry.partition};
            kafka:PartitionOffset offset = {partition: topicPartition, offset: entry.offset};
            var commitAcceptedResult = acceptedReviewConsumer->commitOffset([offset], duration = 10000);
        }
    }
}
