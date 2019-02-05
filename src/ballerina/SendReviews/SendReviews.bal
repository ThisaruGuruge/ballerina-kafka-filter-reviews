import ballerina/io;
import ballerina/http;
import ballerina/runtime;

type Review record {
    string title;
    string content;
    string courseName;
    int rating;
    string userName;
    string userId;
};

http:Client clientEndpoint = new("http://localhost:30005");

public function main(string... args) {
    http:Request req = new;
    Review review = {
        title: "Awesome",
        content: "Great course to follow",
        courseName: "Ballerina",
        rating: 10,
        userName: "Thisaru",
        userId: "thisaruG"
    };

    json msg = {};
    msg.header = "review";
    var reviewJson = json.convert(review);
    if (reviewJson is error) {
        msg.body = "";
    } else {
        msg.body = reviewJson;
    }
    req.setJsonPayload(msg);

    while(true) {
        var response = clientEndpoint->put("/reviews/submitReview", req);
        if (response is error) {
            io:print("[Send Reviews]\t[ERROR]\tResponse is invalid: ");
            io:println(response);
        } else {
            io:println("\nPUT request:");
            var resMsg = response.getJsonPayload();
            if (resMsg is error) {
                io:print("[Send Reviews]\t[ERROR]\tJSON payload is invalid: ");
                io:println(resMsg);
            } else {
                io:print("[Send Reviews]\t[INFO]\t");
                io:println(resMsg);
            }
        }
        runtime:sleep(2000);
    }
}
