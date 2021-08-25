/*
 * Copyright (c) 2021 XdevL. All rights reserved.
 *
 * This work is licensed under the terms of the MIT license.
 * For a copy, see <https://opensource.org/licenses/MIT>.
 */

import {DynamoDB} from "aws-sdk";
import {Initializer, map, typeOf} from "./utils";
import {BaseOptions, DynamoDao, DynamoIndex} from "./dao";
import {DynamoSerializer} from "./serializer";
import {QueryInput} from "aws-sdk/clients/dynamodb";

interface ListOptions extends BaseOptions<QueryInput> {
    lookup?: string;
}

class Entity<T> {

    public static create<T>(ENTITY_TYPE: string, ENTITY_ID: string,
                            ENTITY_LOOKUP: string, ENTITY_VALUE: T): Entity<T> {
        return new Entity({ENTITY_TYPE, ENTITY_ID, ENTITY_LOOKUP, ENTITY_VALUE});
    }

    public readonly ENTITY_TYPE: string;
    public readonly ENTITY_ID: string;
    public readonly ENTITY_LOOKUP: string;
    public readonly ENTITY_VALUE: T;
    constructor(params: Initializer<Entity<any>>) {
        this.ENTITY_TYPE = params.ENTITY_TYPE;
        this.ENTITY_ID = params.ENTITY_ID;
        this.ENTITY_LOOKUP = params.ENTITY_LOOKUP;
        this.ENTITY_VALUE = params.ENTITY_VALUE;
    }
}

export class EntityDao<T> {

    private readonly dao: DynamoDao<DynamoSerializer<Entity<T>, any>, "ENTITY_TYPE", "ENTITY_ID">;
    private readonly lookupIndex: DynamoIndex<DynamoSerializer<Entity<T>, any>, "ENTITY_TYPE", "ENTITY_LOOKUP">

    constructor(dynamoDb: DynamoDB, tableName: string, serializer: DynamoSerializer<T, any>, private readonly entityType: string,
                private readonly id: (value: T) => string, private readonly lookup: (value: T) => string) {

        this.dao = new DynamoDao(dynamoDb, tableName, new DynamoSerializer (typeOf<Entity<any>>(), () => ({
                ENTITY_ID: DynamoSerializer.string(),
                ENTITY_LOOKUP: DynamoSerializer.string(),
                ENTITY_TYPE: DynamoSerializer.string(),
                ENTITY_VALUE: serializer,
            }), (attrs) => new Entity(attrs)), ["ENTITY_TYPE", "ENTITY_ID"]);

        this.lookupIndex = this.dao.localIndex("ENTITY_LOOKUP", "ENTITY_LOOKUP");
    }

    public async persist(entities: AsyncGenerator<T>): Promise<void> {
        return this.dao.persist(map(entities, (entity) => this.wrap(entity)));
    }

    public async *list(options?: ListOptions): AsyncGenerator<T> {
        const {lookup, ...lookupOptions} = options ?? {};
        yield *map(this.lookupIndex.lookup(this.entityType, {
            condition: lookup ? {matcher: "begins_with", value: lookup} : undefined,
            ...lookupOptions
        }), (entity) => entity.ENTITY_VALUE);
    }

    public async get(keyValue: string): Promise<T | undefined> {
        return this.dao.get([this.entityType, keyValue])
            .then((entity) => entity ? entity.ENTITY_VALUE : undefined);
    }

    public delete(keyValue: string): Promise<void> {
        return this.dao.delete([this.entityType, keyValue]);
    }

    private wrap(value: T): Entity<T> {
        return Entity.create(this.entityType, this.id(value), this.lookup(value), value);
    }

    public static synchronise(dynamoDb: DynamoDB, tableName: string, throughput?: DynamoDB.ProvisionedThroughput): Promise<void> {
        const template = new EntityDao<undefined>(dynamoDb, tableName, new DynamoSerializer(typeOf<undefined>(), () => ({}), () => undefined), "", () => "", () => "");
        return template.dao.createTableIfNeeded(throughput || {
                ReadCapacityUnits: 1,
                WriteCapacityUnits: 1,
            }, template.lookupIndex);
    }
}