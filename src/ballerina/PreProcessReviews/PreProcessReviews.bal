import ballerina/io;
import wso2/kafka;
import ballerina/http;
import ballerina/math;

kafka:ProducerConfig reviewProducerConfigs = {
    bootstrapServers: "localhost:9092",
    clientID: "preprocess-review-producer",
    acks: "all",
    noRetries: 3
};

kafka:SimpleProducer reviewProducer = new(reviewProducerConfigs);

type Review record {
    string title;
    string content;
    string courseName;
    int rating;
    string userName;
    string userId;
    !...;
};

type ProcessedReview record {
    boolean isFraud?;
    Review review?;
    !...;
};

// Listen to port 30005 for recods
listener http:Listener reviewsEP = new(30005);

@http:ServiceConfig { basePath: "/reviews" }
service getReviews on reviewsEP {
    @http:ResourceConfig { methods: ["PUT"], path: "/submitReview" }
    resource function receiveReview(http:Caller caller, http:Request req) {
        string topic = "pre-processed-reviews";
        json message;
        json successResponse = { success: "true", message: "Seccessfully received review" };
        json failureResponse = { success: "false", message: "Receiving review failed" };
        http:Response res = new;

        var requestPayload = req.getJsonPayload();
        if (requestPayload is error) {
            res.setJsonPayload(failureResponse);
            var respondResult = caller->respond(res);
            if (respondResult is error) {
                io:print("[Preprocess]\t[ERROR]\tSending error response is failed: ");
                io:println(respondResult);
            } else {
                io:println("[Preprocess]\t[INFO]\tError response sent");
            }
        } else {
            message = requestPayload;
            io:println("[Preprocess]\t[INFO]\tPre-Processing: ");

            string header = message.header.toString();
            res.setJsonPayload(successResponse);
            var respondResult = caller->respond(res);
            if (respondResult is error) {
                io:print("[Preprocess]\t[ERROR]\tResponding Failed: ");
                io:println(respondResult);
            } else {
                io:println("[Preprocess]\t[INFO]\tSuccessfully sent the response.");
            }
            if (header == "review") {
                var review = Review.convert(message.body);
                if (review is Review) {
                    ProcessedReview processedReview = {};
                    processedReview.review = review;
                    io:println("[Preprocess]\t[INFO]\treview is set in payload.");
                    int randomInt = math:randomInRange(0, 2);
                    if (randomInt == 0) {
                        processedReview.isFraud = false;
                        io:println("[Preprocess]\t[INFO]\tReview should be accepted.");
                    } else {
                        processedReview.isFraud = true;
                        io:println("[Preprocess]\t[INFO]\tReview should be rejected.");
                    }
                    var msg = json.convert(processedReview);
                    if (msg is error) {
                        io:println("[Preprocess]\t[ERROR]\tCouldn't convert to Review json");
                    } else {
                        byte[] messageToPublish = msg.toString().toByteArray("UTF-8");
                        var sendResult = reviewProducer->send(messageToPublish, topic);
                        if (sendResult is error) {
                            io:print("[Preprocess]\t[ERROR]\tSending Received Review failed: ");
                            io:println(sendResult);
                        } else {
                            io:println("[Preprocess]\t[INFO]\tSent Received Review");
                        }
                    }
                } else {
                    io:println("[Preprocess]\t[ERROR]\tCouldn't convert to record type: Review");
                }
            } else {
                io:println("[Preprocess]\t[ERROR]\tHeader is invalid");
            }
        }
        io:println("\n");
    }
}
