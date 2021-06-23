/*
 * Copyright (c) 2021 XdevL. All rights reserved.
 *
 * This work is licensed under the terms of the MIT license.
 * For a copy, see <https://opensource.org/licenses/MIT>.
 */

import {DynamoDB} from "aws-sdk";
import {Field} from "./utils";
import {DynamoSerializer, IDynamoSerializer} from "./serializer";
import { ExpressionAttributeValueMap, QueryInput, QueryOutput, ScanInput, ScanOutput} from "aws-sdk/clients/dynamodb";

async function *chunksOf<T>(values: AsyncGenerator<T>, chunkSize: number): AsyncGenerator<T[]> {
    let chunk = [];
    for await (const value of values) {
        if (chunk.length === chunkSize && chunkSize > 0) {
            yield chunk;
            chunk = [];
        }
        chunk.push(value);
    }
    yield chunk;
}

type Matcher = "=" | "<" | "<=" | ">" | ">=" | "between" | "begins_with";

interface Condition<T extends DynamoSerializer<any, any>, PK extends Indexable<T>, M extends Matcher> {
    key: PK;
    matcher: M;
    value: M extends "between" ? [SerializedType<T>[PK], SerializedType<T>[PK]] : SerializedType<T>[PK];
}

const conditionToString = (matcher: Matcher, name: string) => {
    if (matcher === "begins_with") {
        return `begins_with(${name}, :value)`;
    } else if (matcher === "between") {
        return `${name} between :from and :to`;
    } else {
        return `${name} ${matcher} :value`;
    }
}

type SerializedType<T extends DynamoSerializer<any, any>> = T extends DynamoSerializer<infer T, any> ? T : never;
type SerializerType<T extends DynamoSerializer<any, any>> = T extends DynamoSerializer<any, infer T> ? T : never;
type Indexable<T extends DynamoSerializer<any, any>> = keyof SerializedType<T> & Field<SerializerType<T>, IDynamoSerializer<any, "S"> | IDynamoSerializer<any, "N">>;

interface Key<T extends DynamoSerializer<any, any>, PK extends Indexable<T>, SK extends Exclude<Indexable<T>, PK>> {
    primaryKey: SerializedType<T>[PK],
    sortKey: SerializedType<T>[SK]
}

interface KeyWithIndex<T extends DynamoSerializer<any, any>, PK extends Indexable<T>, SK extends Exclude<Indexable<T>, PK>,
    I extends Exclude<Indexable<T>, PK | SK>> extends Key<T, PK, SK> {
    index: SerializedType<T>[I]
}

interface ListOptions<T extends DynamoSerializer<any, any>, PK extends Indexable<T>, SK extends Exclude<Indexable<T>, PK>,
    I extends Exclude<Indexable<T>, PK | SK>, U extends I|void> {
    index?: U,
    startFrom?: U extends I ?  KeyWithIndex<T, PK, SK, U> : Key<T, PK, SK>,
    overwrite?: (input: ScanInput) => ScanInput,
}


interface LookupOptions<T extends DynamoSerializer<any, any>, PK extends Indexable<T>, SK extends Exclude<Indexable<T>, PK>,
        I extends Exclude<Indexable<T>, PK | SK>, U extends SK|I, M extends Matcher> {
    condition?: Condition<T, U, M>
    startFrom?: U extends I ? KeyWithIndex<T, PK, SK, U> : Key<T, PK, SK>,
    overwrite?: (input: QueryInput) => QueryInput,
}


// TODO: should we allow primary keys ?
export class DynamoDao<T extends DynamoSerializer<any, any>, PK extends Indexable<T>, SK extends Exclude<Indexable<T>, PK> , I extends Exclude<Indexable<T>, PK | SK>> {

    constructor(private readonly tableName: string,
                private readonly primaryKey: PK,
                private readonly sortKey: SK,
                private readonly indexes: I[],
                private readonly serializer: T) {
    }

    public async persist(dynamoDb: DynamoDB, entities: AsyncGenerator<SerializedType<T>>): Promise<void> {
        for await (const chunk of chunksOf(entities, 25)) {
            await dynamoDb.batchWriteItem({ RequestItems: { [this.tableName]: chunk.map((entity ) => ({
                PutRequest: {Item: this.serializer.serialize(entity).M}})),
            }}).promise();
        }
    }

    public async *list<U extends I|void = void>(dynamoDb: DynamoDB, options?: ListOptions<T, PK, SK, I, U>): AsyncGenerator<SerializedType<T>> {
        const withIndex = options?.startFrom as KeyWithIndex<T, PK, SK, I>;
        const overwrite = options?.overwrite ?? ((params: ScanInput) => params);
        yield *this.fetch(overwrite({
            TableName: this.tableName,
            IndexName: options?.index as string,
            ExclusiveStartKey: options?.startFrom ? {
                [this.primaryKey]: this.serializeValue(this.primaryKey, options.startFrom.primaryKey),
                [this.sortKey]: this.serializeValue(this.sortKey, options.startFrom.sortKey),
                ...(options?.index ? {[options.index as string]: this.serializeValue(options.index as I, withIndex.index)} : {})
            } : undefined
        }), (params) => dynamoDb.scan(params).promise());
    }

    public async *lookup<U extends I|SK, M extends Matcher>(dynamoDb: DynamoDB, primaryKey: SerializedType<T>[PK], options?: LookupOptions<T, PK, SK, I, U, M>): AsyncGenerator<SerializedType<T>> {
        const index = options?.condition && options.condition.key !== this.sortKey ? options.condition.key as I : undefined;
        const withIndex = options?.startFrom as KeyWithIndex<T, PK, SK, I>;
        const overwrite = options?.overwrite ?? ((params: ScanInput) => params);
        const pkCondition = "#pk = :pk";
        yield *this.fetch(overwrite({
            ExpressionAttributeNames: {
                "#pk": this.primaryKey as string,
                ...(options?.condition ? {"#sk": options.condition.key as string} : {})
            },
            ExpressionAttributeValues: {
                ":pk":  this.serializeValue(this.primaryKey, primaryKey),
                ...(options?.condition ? this.attributeValues(options.condition) : {} )
            },
            KeyConditionExpression: options?.condition ? `${pkCondition} and ${conditionToString(options.condition.matcher, "#sk")}` : pkCondition,
            ExclusiveStartKey: options?.startFrom ? {
                [this.primaryKey]: this.serializeValue(this.primaryKey, options.startFrom.primaryKey),
                [this.sortKey]: this.serializeValue(this.sortKey, options.startFrom.sortKey),
                ...(index ? {[index as string]: this.serializeValue(index, withIndex.index)} : {})
            } : undefined,
            TableName: this.tableName,
            ...(index ? {IndexName: index as string} : {})
        }), (params) => dynamoDb.query(params).promise());
    }

    public async get(dynamoDb: DynamoDB, primaryKey: SerializedType<T>[PK], sortKey: SerializedType<T>[SK]): Promise<SerializedType<T>|undefined> {
        const result = await dynamoDb.getItem({ Key: {
            [this.primaryKey]: this.serializeValue(this.primaryKey, primaryKey),
            [this.sortKey]: this.serializeValue(this.sortKey, sortKey),
        }, TableName: this.tableName }).promise();

        return result.Item ? this.serializer.deserialize({M: result.Item}) : undefined;
    }

    public async delete(dynamoDb: DynamoDB, primaryKey: SerializedType<T>[PK], sortKey: SerializedType<T>[SK]): Promise<void> {
        await dynamoDb.deleteItem({ Key: {
            [this.primaryKey]: this.serializeValue(this.primaryKey, primaryKey),
            [this.sortKey]: this.serializeValue(this.sortKey, sortKey),
        }, TableName: this.tableName}).promise();
    }

    public async createTableIfNeeded(dynamoDb: DynamoDB, throughput: DynamoDB.ProvisionedThroughput): Promise<void> {
        const spec = this.buildTableSpec(throughput);
        const tableParam = {TableName: spec.TableName};
        try {
            const status = await dynamoDb.describeTable(tableParam).promise();
            if (status.Table && status.Table.TableStatus === "ACTIVE") {
                return;
            }
        } catch(error) {
            if (error.name === "ResourceNotFoundException") {
                await dynamoDb.createTable(spec).promise();
            } else {
                throw error;
            }
        }
        await dynamoDb.waitFor("tableExists", tableParam).promise();
    }

    public async *fetch<P extends QueryInput|ScanInput>(params: P, callback: (params: P) => Promise<QueryOutput|ScanOutput>): AsyncGenerator<SerializedType<T>> {
        const res = await callback(params);
        const items = res.Items || [];
        for(const item of items) {
            yield this.serializer.deserialize({M: item});
        }

        if (res.LastEvaluatedKey && (params.Limit == undefined || params.Limit > items.length)) {
            yield *this.fetch({
                ...params,
                ExclusiveStartKey: res.LastEvaluatedKey,
                Limit: params.Limit !== undefined ? params.Limit - items.length : undefined
            }, callback);
        }
    }

    private serializeValue<U extends PK|SK|I>(field: U, value: SerializedType<T>[U]): DynamoDB.AttributeValue {
        // TODO: Can we enforce this rather than using an assertion ?
        return this.serializer.serializeField(field as any, value)!
    }

    private typeOf<U extends PK|SK|I>(field: U): (keyof DynamoDB.AttributeValue) {
        return this.serializer.typeOf(field as any)!;
    }

    private attributeValues<U extends I|SK, M extends Matcher>(condition: Condition<T, U, M>): ExpressionAttributeValueMap {
        const between = condition as Condition<T, U, "between">;
        const notBetween = condition as Condition<T, U, Exclude<M, "between">>;
        return condition.matcher === "between" ? {
            ":from": this.serializeValue(condition.key, between.value[0]),
            ":to": this.serializeValue(condition.key, between.value[1])
        } : {
            ":value": this.serializeValue(condition.key, notBetween.value)
        };
    }

    private buildTableSpec(throughput: DynamoDB.ProvisionedThroughput): DynamoDB.CreateTableInput {
        return {
            AttributeDefinitions: [
                {AttributeName: this.primaryKey as string, AttributeType: this.typeOf(this.primaryKey)},
                {AttributeName: this.sortKey as string, AttributeType: this.typeOf(this.sortKey)},
            ].concat(this.indexes.map((index) => ({AttributeName: index as string, AttributeType: this.typeOf(index)}))),
            KeySchema: [
                {AttributeName: this.primaryKey as string, KeyType: "HASH"},
                {AttributeName: this.sortKey as string, KeyType: "RANGE"},
            ],
            LocalSecondaryIndexes: this.indexes.length === 0 ? undefined : this.indexes.map((index) => ({
                IndexName: index as string,
                KeySchema: [
                    {AttributeName: this.primaryKey as string, KeyType: "HASH"},
                    {AttributeName: index as string, KeyType: "RANGE"},
                ],
                Projection: { ProjectionType: "ALL" },
            })),
            ProvisionedThroughput: throughput,
            TableName: this.tableName,
        };
    }
}
