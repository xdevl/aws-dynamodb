/*
 * Copyright (c) 2021 XdevL. All rights reserved.
 *
 * This work is licensed under the terms of the MIT license.
 * For a copy, see <https://opensource.org/licenses/MIT>.
 */

import {DynamoDB} from "aws-sdk";
import {Field, Initializer} from "./utils";

type DynamoType<K extends keyof DynamoDB.AttributeValue> = NonNullable<DynamoDB.AttributeValue[K]>;
type DynamoRecord<K extends keyof DynamoDB.AttributeValue> = Record<K, DynamoType<K>>;

interface IDynamoSerializer<T, K extends keyof DynamoDB.AttributeValue> {
    deserialize: (value: DynamoRecord<K>) => T;
    serialize: (value: T) => DynamoRecord<K>;
}

class DynamoRawSerializer<T extends keyof DynamoDB.AttributeValue>
        implements IDynamoSerializer<DynamoType<T>, T> {

    public static readonly binary = new DynamoRawSerializer("B");
    public static readonly binarySet = new DynamoRawSerializer("BS");
    public static readonly boolean = new DynamoRawSerializer("BOOL");
    public static readonly list = new DynamoRawSerializer("L");
    public static readonly map = new DynamoRawSerializer("M");
    public static readonly null = new DynamoRawSerializer("NULL");
    public static readonly number = new DynamoRawSerializer("N");
    public static readonly numberSet = new DynamoRawSerializer("NS");
    public static readonly string = new DynamoRawSerializer("S");
    public static readonly stringSet = new DynamoRawSerializer("SS");

    constructor(private key: T) {
    }

    public deserialize(value: DynamoRecord<T>): DynamoType<T> {
        return value[this.key];
    }
    public serialize(value: DynamoType<T>): DynamoRecord<T> {
        return {[this.key]: value} as DynamoRecord<T>;
    }
}

type DynamoSerializers<T> = {
    [P in Field<T>]: IDynamoSerializer<T[P], any> | undefined;
};

export class DynamoSerializer<T> implements IDynamoSerializer<T, "M"> {

    public static readonly boolean = (): IDynamoSerializer<boolean, "BOOL"> => DynamoRawSerializer.boolean;

    public static readonly buffer = (): IDynamoSerializer<Buffer, "S"> => ({
        deserialize: (value) => Buffer.from(DynamoRawSerializer.string.deserialize(value), "hex"),
        serialize: (value) => DynamoRawSerializer.string.serialize(value.toString("hex")),
    })

    public static readonly date = (): IDynamoSerializer<Date, "N"> => ({
        deserialize: (value) => new Date(DynamoSerializer.number().deserialize(value)),
        serialize: (value) => DynamoSerializer.number().serialize(value.getTime()),
    })

    public static readonly enum = <T extends {[P in keyof T]: string}>():
            IDynamoSerializer<T, "S"> => DynamoRawSerializer.string as any

    public static list = <T>(serializer: IDynamoSerializer<T, any>): IDynamoSerializer<T[], "L"> => ({
        deserialize: (value) => DynamoRawSerializer.list.deserialize(value)
            .map((item) => serializer.deserialize(item)),
        serialize: (value) => DynamoRawSerializer.list.serialize(value
            .map((item) => serializer.serialize(item))),
    })

    public static readonly number = (): IDynamoSerializer<number, "N"> => ({
        deserialize: (value) => Number(DynamoRawSerializer.number.deserialize(value)),
        serialize: (value) => DynamoRawSerializer.number.serialize(String(value)),
    })

    public static readonly optional = <T> (serializer: IDynamoSerializer<T, any>): IDynamoSerializer<T | undefined, any> => ({
        deserialize: (value) => value === undefined || DynamoRawSerializer.null.deserialize(value) ? undefined : serializer.deserialize(value),
        serialize: (value) => value === undefined ? DynamoRawSerializer.null.serialize(true) : serializer.serialize(value),
    })

    public static readonly string = (): IDynamoSerializer<string, "S"> => DynamoRawSerializer.string;

    public static readonly stringSet = (): IDynamoSerializer<string[], "SS"> => DynamoRawSerializer.stringSet;

    private static assign<T, K extends keyof T>(target: T, key: K, value: T[K] | undefined): T {
        return value === undefined ? target: Object.assign(target, {[key]: value});
    }

    private readonly serializers: DynamoSerializers<T>;

    // Note: the define callback allows to enforce strict typing and prevent the definition of serializers that don't have matching properties
    constructor(serializers: (define: (serializers: DynamoSerializers<T>) => DynamoSerializers<T>, self: DynamoSerializer<T>) => DynamoSerializers<T>,
                private readonly factory: (attrs: Initializer<T>) => T) {
        this.serializers = serializers((values) => values, this);
    }

    public serialize(entity: T): DynamoRecord<"M"> {
        return DynamoRawSerializer.map.serialize(this.fields.reduce((acc, field) =>
            DynamoSerializer.assign(acc, field as string, this.serializeField(field, entity[field])),
                {} as DynamoDB.MapAttributeValue));
    }

    public deserialize(attrs: DynamoRecord<"M">): T {
        return this.factory(this.fields.reduce((acc, field) =>
            DynamoSerializer.assign(acc, field, this.deserializeField(field, attrs.M[field as string])),
                {} as Initializer<T>));
    }

    public serializeField<F extends Field<T>>(field: F, value: T[F]): DynamoDB.AttributeValue | undefined {
        const serializer = this.serializers[field];
        return serializer?.serialize(value);
    }

    public deserializeField<F extends Field<T>>(field: F, value: DynamoDB.AttributeValue): T[F] | undefined {
        const serializer = this.serializers[field];
        return serializer?.deserialize(value);
    }

    private get fields(): Field<T>[] {
        return Object.keys(this.serializers) as unknown as Field<T>[];
    }
}