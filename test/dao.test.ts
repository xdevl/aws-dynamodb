import {DynamoDao} from "../lib/dao";
import {DynamoSerializer} from "../lib/serializer";
import {collect, Initializer, generator, typeOf} from "../lib/utils";

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

const productSerializer = new DynamoSerializer(typeOf<Product>(), () => ({
    type: DynamoSerializer.string(),
    code: DynamoSerializer.number(),
    price: DynamoSerializer.number()
}), (attrs) => new Product(attrs));

const setupDao = (mock: any) => new DynamoDao(mock, "ProductTable", productSerializer, ["type", "code"]);

const dynamock = <T extends string>(name: T, ...values: any[]) => {
    const mock = jest.fn();
    values.forEach((value) => mock.mockReturnValueOnce({promise: () => Promise.resolve(value)}));
    return { [name]: mock } as Record<T, jest.Mock>;
}

test("Can list", async () => {
    const dao = setupDao(dynamock("scan", {
        Items: [
            { type: {S: "soap"}, code: {N: "1"}, price: {N: "2.5"} },
            { type: {S: "chicken"}, code: {N: "2"}, price: {N: "7"} }
        ]
    }));

    const values = await collect(dao.list());

    expect(values).toStrictEqual([
        new Product({type: "soap", code: 1, price: 2.5}),
        new Product({type: "chicken", code: 2, price: 7})
    ]);
});

test("Can list chunked result", async () => {
    const dao = setupDao(dynamock("scan", {
        Items: [
            { type: {S: "soap"}, code: {N: "1"}, price: {N: "2.5"} },
        ],
        LastEvaluatedKey: {}
    }, {
        Items: [
            { type: {S: "chicken"}, code: {N: "2"}, price: {N: "7"} }
        ]
    }));

    const values = await collect(dao.list());

    expect(values).toStrictEqual([
        new Product({type: "soap", code: 1, price: 2.5}),
        new Product({type: "chicken", code: 2, price: 7})
    ]);
});

test("Can persist", async () => {
    const mock = dynamock("batchWriteItem", {});
    const dao = setupDao(mock);

    await dao.persist(generator(
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
    const dao = setupDao(mock);

    const values = await collect(dao.lookup("cucumber"));

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
    const dao = setupDao(mock);

    const values = await collect(dao.lookup("cheese", {
        condition: {matcher: "<", value: 0}
    }));

    expect(values).toStrictEqual([]);
    expect(mock.query).toBeCalledWith({
        ExpressionAttributeNames: {
            "#pk": "type",
            "#sk": "code"
        },
        ExpressionAttributeValues: {
            ":pk": {"S": "cheese"},
            ":value": {"N": "0"}
        },
        KeyConditionExpression: "#pk = :pk and #sk < :value",
        TableName: "ProductTable",
    });    
});

test("Can lookup index", async () => {
    const mock = dynamock("query", {});
    const dao = setupDao(mock);
    const priceIndex = dao.localIndex("priceIndex", "price");

    const values = await collect(priceIndex.lookup("cheese", {
        condition: {matcher: "between", value: [5, 10]}
    }));

    expect(values).toStrictEqual([]);
    expect(mock.query).toBeCalledWith({
        ExpressionAttributeNames: {
            "#pk": "type",
            "#sk": "price"
        },
        ExpressionAttributeValues: {
            ":pk": {"S": "cheese"},
            ":from": {"N": "5"},
            ":to": {"N": "10"}
        },
        IndexName: "priceIndex",
        KeyConditionExpression: "#pk = :pk and #sk between :from and :to",
        TableName: "ProductTable",
    });    
});

test("Can get", async () => {
    const mock = dynamock("getItem", {});
    const dao = setupDao(mock);

    const value = await dao.get(["tomatoe", 64]);

    expect(value).toBeUndefined();
    expect(mock.getItem).toBeCalledWith({
        Key: {
            type: {S: "tomatoe"},
            code: {N: "64"}
        },
        TableName: "ProductTable"
    });    
});