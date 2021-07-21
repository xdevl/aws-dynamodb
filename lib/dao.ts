/*
 * Copyright (c) 2021 XdevL. All rights reserved.
 *
 * This work is licensed under the terms of the MIT license.
 * For a copy, see <https://opensource.org/licenses/MIT>.
 */

import {DynamoDB} from "aws-sdk";
import {Field, IsStrictlyAny} from "./utils";
import {DynamoSerializer, IDynamoSerializer} from "./serializer";
import { ExpressionAttributeValueMap, QueryInput, QueryOutput, ScanInput, ScanOutput} from "aws-sdk/clients/dynamodb";

const arrayable = <T>(value: T | T[]): T[] => value instanceof Array ? value : [value];

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

type SerializedType<T extends DynamoSerializer<any, any>> = T extends DynamoSerializer<infer T, any> ? T : never;
type SerializerType<T extends DynamoSerializer<any, any>> = T extends DynamoSerializer<any, infer T> ? T : never;
type Indexable<T extends DynamoSerializer<any, any>> = keyof SerializedType<T> & Field<SerializerType<T>, IDynamoSerializer<any, "S"> | IDynamoSerializer<any, "N">>;
type SortKeySpec<T extends DynamoSerializer<any, any>, PK extends Indexable<T>> = IsStrictlyAny<PK> extends false ? Exclude<Indexable<T>, PK> : Indexable<T>;
type KeySpec<T extends DynamoSerializer<any, any>, PK extends Indexable<T>, SK extends SortKeySpec<T, PK>> = PK | [PK, SK];
// see: https://stackoverflow.com/questions/53984650/typescript-never-type-inconsistently-matched-in-conditional-type
type KeyType<T extends DynamoSerializer<any, any>, PK extends Indexable<T>, SK extends SortKeySpec<T, PK>> = [SK] extends [never] ? SerializedType<T>[PK] : [SerializedType<T>[PK], SerializedType<T>[SK]];


type Matcher = "=" | "<" | "<=" | ">" | ">=" | "between" | "begins_with";

interface Condition<T extends DynamoSerializer<any, any>, K extends Indexable<T>, M extends Matcher> {
    matcher: M;
    value: M extends "between" ? [SerializedType<T>[K], SerializedType<T>[K]] : SerializedType<T>[K];
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

interface BaseOptions<T> {
    overwrite?: (input: T) => T,
    limit?: number,
    startFrom?: any
}

type ListOptions = BaseOptions<ScanInput>

interface LookupOptions<T extends DynamoSerializer<any, any>, K extends Indexable<T>, M extends Matcher> extends BaseOptions<QueryInput> {
    condition?: Condition<T, K, M>
}

enum IndexType {
    LOCAL, GLOBAL
}

export class DynamoIndex<T extends DynamoSerializer<any, any>, PK extends Indexable<T>, SK extends SortKeySpec<T, PK>> {

    readonly keySchema: DynamoDB.KeySchema;

    constructor(readonly tableName: string, readonly serializer: T, readonly keyspec: KeySpec<T, PK, SK>, readonly type = IndexType.GLOBAL, readonly name?: string) {
        this.keySchema = arrayable(this.keyspec).map((name, index) => ({
            AttributeName: name as string, KeyType: index == 0 ? "HASH" : "RANGE"
        }));
    }

    public async *list(dynamoDb: DynamoDB, options?: ListOptions): AsyncGenerator<SerializedType<T>> {
        const overwrite = options?.overwrite ?? ((params: ScanInput) => params);
        yield *this.fetch(overwrite({
            TableName: this.tableName,
            IndexName: this.name,
            ExclusiveStartKey: options?.startFrom,
            Limit: options?.limit,
        }), (params) => dynamoDb.scan(params).promise());
    }

    public async *lookup<M extends Matcher>(dynamoDb: DynamoDB, partitionKey: SerializedType<T>[PK], options?: LookupOptions<T, SK, M>): AsyncGenerator<SerializedType<T>> {
        const overwrite = options?.overwrite ?? ((params: QueryInput) => params);
        const pkCondition = "#pk = :pk";
        yield *this.fetch(overwrite({
            TableName: this.tableName,
            IndexName: this.name,
            ExclusiveStartKey: options?.startFrom,
            Limit: options?.limit,
            ExpressionAttributeNames: {
                "#pk": this.partitionKey() as string,
                ...(options?.condition ? {"#sk": this.sortKey() as string} : {})
            },
            ExpressionAttributeValues: {
                ":pk":  this.serializeValue(this.partitionKey(), partitionKey),
                ...(options?.condition ? this.attributeValues(options.condition) : {} )
            },
            KeyConditionExpression: options?.condition ? `${pkCondition} and ${conditionToString(options.condition.matcher, "#sk")}` : pkCondition,
        }), (params) => dynamoDb.query(params).promise());
    }

    // TODO: add a callback to notify of LastEvaluatedKey
    public async *fetch<P extends QueryInput|ScanInput>(params: P, callback: (params: P) => Promise<QueryOutput|ScanOutput>): AsyncGenerator<SerializedType<T>> {
        const res = await callback(params);
        const items = res.Items || [];
        for(const item of items) {
            yield this.serializer.deserialize({M: item});
        }

        console.log("LastEvaluatedKey: " + JSON.stringify(res.LastEvaluatedKey));
        if (res.LastEvaluatedKey && (params.Limit == undefined || params.Limit > items.length)) {
            yield *this.fetch({
                ...params,
                ExclusiveStartKey: res.LastEvaluatedKey,
                Limit: params.Limit !== undefined ? params.Limit - items.length : undefined
            }, callback);
        }
    }

    protected serializeValue<U extends PK|SK>(field: U, value: SerializedType<T>[U]): DynamoDB.AttributeValue {
        // TODO: Can we enforce this rather than using an assertion ?
        return this.serializer.serializeField(field as any, value)!
    }

    private partitionKey(): PK {
        return this.keyspec instanceof Array ? (this.keyspec as [PK, SK])[0] : (this.keyspec as PK);
    }

    private sortKey(): SK {
        return (this.keyspec as [PK, SK])[1];
    }

    private attributeValues<M extends Matcher>(condition: Condition<T, SK, M>): ExpressionAttributeValueMap {
        const between = condition as Condition<T, SK, "between">;
        const notBetween = condition as Condition<T, SK, Exclude<M, "between">>;
        return condition.matcher === "between" ? {
            ":from": this.serializeValue(this.sortKey(), between.value[0]),
            ":to": this.serializeValue(this.sortKey(), between.value[1])
        } : {
            ":value": this.serializeValue(this.sortKey(), notBetween.value)
        };
    }
}

export class DynamoDao<T extends DynamoSerializer<any, any>, PK extends Indexable<T>, SK extends SortKeySpec<T, PK> = never>
    extends DynamoIndex<T, PK, SK> {

    constructor(tableName: string, serializer: T, keyspec: KeySpec<T, PK, SK>) {
        super(tableName, serializer, keyspec);
    }

    public async persist(dynamoDb: DynamoDB, entities: AsyncGenerator<SerializedType<T>>): Promise<void> {
        for await (const chunk of chunksOf(entities, 25)) {
            await dynamoDb.batchWriteItem({ RequestItems: { [this.tableName]: chunk.map((entity ) => ({
                PutRequest: {Item: this.serializer.serialize(entity).M}})),
            }}).promise();
        }
    }

    public async get(dynamoDb: DynamoDB, key: KeyType<T, PK, SK>): Promise<SerializedType<T>|undefined> {
        const result = await dynamoDb.getItem({ Key: this.dynamoDbKey(key), TableName: this.tableName }).promise();

        return result.Item ? this.serializer.deserialize({M: result.Item}) : undefined;
    }

    public async delete(dynamoDb: DynamoDB, key: KeyType<T, PK, SK>): Promise<void> {
        await dynamoDb.deleteItem({ Key: this.dynamoDbKey(key), TableName: this.tableName}).promise();
    }

    public localIndex<I extends SortKeySpec<T, PK>>(name: string, spec: I): DynamoIndex<T, PK, I>{
        const pk = this.keyspec instanceof Array ? this.keyspec[0] : this.keyspec;
        return new DynamoIndex(this.tableName, this.serializer, [pk, spec], IndexType.LOCAL, name);
    }

    public globalIndex<IPK extends Indexable<T>, ISK extends SortKeySpec<T, IPK> = never>(name: string, spec: KeySpec<T, IPK, ISK>): DynamoIndex<T, IPK, ISK> {
        return new DynamoIndex(this.tableName, this.serializer, spec, IndexType.GLOBAL, name);
    }

    public async createTableIfNeeded(dynamoDb: DynamoDB, throughput: DynamoDB.ProvisionedThroughput, ...indexes: DynamoIndex<T, any, any>[]): Promise<void> {
        const spec = this.buildTableSpec(throughput, ...indexes);
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

    private dynamoDbKey(key: KeyType<T, PK, SK>): DynamoDB.Key {
        return arrayable(this.keyspec).reduce((acc, name, index) => Object.assign(acc, {[name]: this.serializeValue(name, arrayable(key)[index])}), {});
    }

    private typeOf(field: string): (keyof DynamoDB.AttributeValue) {
        return this.serializer.typeOf(field)!;
    }

    public buildTableSpec(throughput: DynamoDB.ProvisionedThroughput, ...indexes: DynamoIndex<T, any, any>[]): DynamoDB.CreateTableInput {
        const attributes = new Set<string>();
        arrayable(this.keyspec).forEach((attribute) => attributes.add(attribute as string));
        indexes.forEach((index) => arrayable(index.keyspec).forEach((attribute) => attributes.add(attribute as string)));
        const localIndexes = indexes.filter((index) => index.type == IndexType.LOCAL);
        const globalIndexes = indexes.filter((index) => index.type == IndexType.GLOBAL);
        return {
            AttributeDefinitions: Array.from(attributes).map((key) => ({
                AttributeName: key, AttributeType: this.typeOf(key)
            })),
            KeySchema: this.keySchema,
            LocalSecondaryIndexes: localIndexes.length < 1 ? undefined : localIndexes.map((index) => ({
                IndexName: index.name ?? arrayable(index.keyspec).join("_"),
                KeySchema: index.keySchema,
                Projection: { ProjectionType: "ALL" }
            })),
            GlobalSecondaryIndexes: globalIndexes.length < 1 ? undefined : globalIndexes.map((index) => ({
                IndexName: index.name ?? arrayable(index.keyspec).join("_"),
                KeySchema: index.keySchema,
                Projection: { ProjectionType: "ALL" },
                ProvisionedThroughput: throughput
            })),
            ProvisionedThroughput: throughput,
            TableName: this.tableName,
        };
    }
}