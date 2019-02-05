import ballerina/io;
import wso2/kafka;
import ballerina/encoding;

string topicPreprocessedReviews = "pre-processed-reviews";
string topicRejectedReviews = "rejected-reviews";
string topicAcceptedReviews = "accepted-reviews";

kafka:ConsumerConfig consumerConfigs = {
    bootstrapServers: "localhost:9092",
    groupId: "review-filter-consumer",
    topics: [topicPreprocessedReviews]
};

listener kafka:SimpleConsumer reviewConsumer = new(consumerConfigs);

kafka:ProducerConfig producerConfigs = {
    bootstrapServers: "localhost:9092",
    clientID: "filter-review-producer",
    noRetries: 3
};

kafka:SimpleProducer filteredReviewsProducer = new(producerConfigs);

type Review record {
    string title;
    string content;
    string courseName;
    int rating;
    string userName;
    string userId;
};

type ProcessedReview record {
    boolean isFraud;
    Review review;
};

service filterReviewService on reviewConsumer {
    resource function onMessage (kafka:SimpleConsumer simpleConsumer, kafka:ConsumerRecord[] records) {
        foreach var entry in records {
            byte[] serializedMsg = entry.value;
            string msg = encoding:byteArrayToString(serializedMsg);

            io:StringReader receivedString = new(msg);
            var receivedReviewJson = receivedString.readJson();
            if (receivedReviewJson is error) {
                io:print("[Filter Reviews]\t[ERROR]\tError Parsing Json: ");
                io:println(receivedReviewJson);
            } else {
                string topicToPublish = "";
                var receivedReview = ProcessedReview.convert(receivedReviewJson);
                if (receivedReview is error) {
                    io:print("[Filter Reviews]\t[ERROR]\tCouldn't convert recevied review json to Record type - ReceivedReview: ");
                    io:println(receivedReview);
                } else {
                    if (receivedReview.isFraud) {
                        topicToPublish = topicRejectedReviews;
                        io:println("[Filter Reviews]\t[INFO]\tFraudelent review found");
                    } else if (!receivedReview.isFraud) {
                        topicToPublish = topicAcceptedReviews;
                        io:println("[Filter Reviews]\t[INFO]\tReview is fine. Sending to publish");
                    }

                    byte[] reviewToSend = receivedReviewJson.review.toString().toByteArray("UTF-8");
                    io:println("[Filter Reviews]\t[INFO]\tTry to send review to the topic: " + topicToPublish);
                    var sendResult = filteredReviewsProducer->send(reviewToSend, topicToPublish);
                    io:println("[Filter Reviews]\t[INFO]\tReview sent.");
                    if (sendResult is error) {
                        io:print("[Filter Reviews]\t[ERROR]\tSending the review failed: ");
                        io:println(sendResult);
                    } else {
                        io:println("[Filter Reviews]\t[INFO]\tReview Published to: \"" + topicToPublish + "\"");
                    }
                }
            }
            io:println("[Filter Reviews]\t[INFO]\tReceived Message:\n" + msg + "\n\n");
        }
    }
}
