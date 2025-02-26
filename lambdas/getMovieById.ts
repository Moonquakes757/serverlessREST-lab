import { APIGatewayProxyHandlerV2 } from "aws-lambda";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, GetCommand, QueryCommand } from "@aws-sdk/lib-dynamodb";

const ddbDocClient = createDDbDocClient();

export const handler: APIGatewayProxyHandlerV2 = async (event, context) => {
  try {
    console.log("[EVENT]", JSON.stringify(event));

    const movieIdParam = event.pathParameters?.movieId;
    if (!movieIdParam) {
      return {
        statusCode: 400,
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ message: "Missing movieId in path" }),
      };
    }

    const movieId = parseInt(movieIdParam, 10);
    if (isNaN(movieId)) {
      return {
        statusCode: 400,
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ message: "Invalid movieId. Must be a number." }),
      };
    }

    const includeCast = event.queryStringParameters?.cast === "true";

    const movieCommand = new GetCommand({
      TableName: process.env.TABLE_NAME,
      Key: { id: movieId },
    });
    const movieResult = await ddbDocClient.send(movieCommand);

    if (!movieResult.Item) {
      return {
        statusCode: 404,
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ message: "Movie not found" }),
      };
    }

    let response = { movie: movieResult.Item };

    if (includeCast) {
      const castCommand = new QueryCommand({
        TableName: process.env.TABLE_NAME,
        IndexName: "movieId-index", 
        KeyConditionExpression: "movieId = :m",
        ExpressionAttributeValues: {
          ":m": movieId,
        },
      });

      const castResult = await ddbDocClient.send(castCommand);
      response = { ...response, cast: castResult.Items || [] };
    }

    return {
      statusCode: 200,
      headers: { "content-type": "application/json" },
      body: JSON.stringify(response),
    };
  } catch (error: any) {
    console.error("Error fetching movie:", JSON.stringify(error));
    return {
      statusCode: 500,
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ error: "Internal server error" }),
    };
  }
};

function createDDbDocClient() {
  const ddbClient = new DynamoDBClient({ region: process.env.REGION });
  const marshallOptions = {
    convertEmptyValues: true,
    removeUndefinedValues: true,
    convertClassInstanceToMap: true,
  };
  const unmarshallOptions = { wrapNumbers: false };
  const translateConfig = { marshallOptions, unmarshallOptions };
  return DynamoDBDocumentClient.from(ddbClient, translateConfig);
}
