/*
 * Copyright (c) 2021 XdevL. All rights reserved.
 *
 * This work is licensed under the terms of the MIT license.
 * For a copy, see <https://opensource.org/licenses/MIT>.
 */

import {DynamoDB} from "aws-sdk";
import {Initializer, map, typeOf} from "./utils";
import {DynamoDao} from "./dao";
import {DynamoSerializer} from "./serializer";

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

    private readonly dao: DynamoDao<DynamoSerializer<Entity<T>, any>, "ENTITY_TYPE", "ENTITY_ID", "ENTITY_LOOKUP">;

    constructor(private readonly dynamoDb: DynamoDB, private readonly entityType: string,
                private readonly id: (value: T) => string, private readonly lookup: (value: T) => string,
                tableName: string, serializer: DynamoSerializer<T, any>) {

        this.dao = new DynamoDao(tableName, "ENTITY_TYPE", "ENTITY_ID", ["ENTITY_LOOKUP"],
            new DynamoSerializer (typeOf<Entity<any>>(), () => ({
                ENTITY_ID: DynamoSerializer.string(),
                ENTITY_LOOKUP: DynamoSerializer.string(),
                ENTITY_TYPE: DynamoSerializer.string(),
                ENTITY_VALUE: serializer,
            }), (attrs) => new Entity(attrs)));
    }

    public async persist(entities: AsyncGenerator<T>): Promise<void> {
        return this.dao.persist(this.dynamoDb, map(entities, (entity) => this.wrap(entity)));
    }

    public async *list(lookup?: string): AsyncGenerator<T> {
        yield *map(this.dao.lookup(this.dynamoDb, this.entityType, {
            condition: lookup ? {key: "ENTITY_LOOKUP", matcher: "begins_with", value: lookup} : undefined,
            overwrite: (input) => ({...input, ScanIndexForward: false})
        }), (entity) => entity.ENTITY_VALUE);
    }

    public async get(keyValue: string): Promise<T | undefined> {
        return this.dao.get(this.dynamoDb, this.entityType, keyValue)
            .then((entity) => entity ? entity.ENTITY_VALUE : undefined);
    }

    public delete(keyValue: string): Promise<void> {
        return this.dao.delete(this.dynamoDb, this.entityType, keyValue);
    }

    private wrap(value: T): Entity<T> {
        return Entity.create(this.entityType, this.id(value), this.lookup(value), value);
    }

    public static synchronise(tableName: string, dynamoDb: DynamoDB, throughput?: DynamoDB.ProvisionedThroughput): Promise<void> {
        return new DynamoDao(tableName, "ENTITY_TYPE", "ENTITY_ID", ["ENTITY_LOOKUP"], undefined as unknown as DynamoSerializer<Entity<any>, any>)
            .createTableIfNeeded(dynamoDb, throughput || {
                ReadCapacityUnits: 1,
                WriteCapacityUnits: 1,
            });
    }
}