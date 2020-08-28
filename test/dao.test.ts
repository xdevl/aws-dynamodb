import {DynamoDao} from "../lib/dao";
import {DynamoSerializer} from "../lib/serializer";
import {collect, Initializer, generator} from "../lib/utils";

class Product {

    public readonly type: string;
    public readonly code: number;
    public readonly price: number;

    constructor(attrs: Initializer<Product>) {
        this.type = attrs.type;
        this.code = attrs.code;
        this.price = attrs.price;
    }
}

const productSerializer = new DynamoSerializer<Product>((define) => define({
    type: DynamoSerializer.string(),
    code: DynamoSerializer.number(),
    price: DynamoSerializer.number()
}), (attrs) => new Product(attrs));

const dao = new DynamoDao("ProductTable", "type", "code", ["price"], productSerializer);

const dynamock = <T extends string>(name: T, result?: any) =>
    ({[name]: jest.fn(() => ({promise: () => Promise.resolve(result)}))} as Record<T, jest.Mock>);

test("Can persist", async () => {
    const mock = dynamock("batchWriteItem");

    await dao.persist(mock as any, generator(
        new Product({type: "soap", code: 1, price: 2.5}),
        new Product({type: "chicken", code: 2, price: 7})
    ));

    expect(mock.batchWriteItem).toBeCalledWith({
        RequestItems: {
            ProductTable: [
                {
                    PutRequest: {
                        Item: { type: {S: "soap"}, code: {N: "1"}, price: {N: "2.5"} }
                    },
                },
                {
                    PutRequest: {
                        Item: { type: {S: "chicken"}, code: {N: "2"}, price: {N: "7"} }
                    }
                }
                
            ]
        }
    });
});

test("Can lookup with primary key", async () => {
    const mock = dynamock("query", {});

    const values = await collect(dao.lookup(mock as any, "cucumber"));

    expect(values).toStrictEqual([]);
    expect(mock.query).toBeCalledWith({
        ExpressionAttributeNames: {
            "#pk": "type"
        },
        ExpressionAttributeValues: {
            ":pk": {"S": "cucumber"}
        },
        KeyConditionExpression: "#pk = :pk",
        TableName: "ProductTable",
    });    
});

test("Can lookup with condition", async () => {
    const mock = dynamock("query", {});

    const values = await collect(dao.lookup(mock as any, "cheese", {
        condition: {key: "price", matcher: "<", value: 0}
    }));

    expect(values).toStrictEqual([]);
    expect(mock.query).toBeCalledWith({
        ExpressionAttributeNames: {
            "#pk": "type",
            "#sk": "price"
        },
        ExpressionAttributeValues: {
            ":pk": {"S": "cheese"},
            ":value": {"N": "0"}
        },
        IndexName: "price",
        KeyConditionExpression: "#pk = :pk and #sk < :value",
        TableName: "ProductTable",
    });    
});

test("Can get", async () => {
    const mock = dynamock("getItem", {});

    const value = await dao.get(mock as any, "tomatoe", 64);

    expect(value).toBeUndefined();
    expect(mock.getItem).toBeCalledWith({
        Key: {
            type: {S: "tomatoe"},
            code: {N: "64"}
        },
        TableName: "ProductTable"
    });    
});