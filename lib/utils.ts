/*
 * Copyright (c) 2021 XdevL. All rights reserved.
 *
 * This work is licensed under the terms of the MIT license.
 * For a copy, see <https://opensource.org/licenses/MIT>.
 */

type FilterFields<T, F> = {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    [K in keyof T]: T[K] extends (...args: any) => any ? never : T[K] extends F ? K : never
};

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export type Field<T, F = any> = FilterFields<T, F>[keyof T];

export type Initializer<T> = Pick<T, Field<T>>;

export async function *map<T, U>(generator: AsyncGenerator<T>, transform: (value:T ) => U): AsyncGenerator<U> {
    for await (const value of generator) {
        yield transform(value);
    }
}

export async function *generator<T>(...values: T[]): AsyncGenerator<T> {
    for (const value of values) {
        yield(value);
    }
}

export const collect = async <T>(generator: AsyncGenerator<T>): Promise<T[]> => {
    const all = [] as T[];
    for await (const value of generator) {
        all.push(value);
    }
    return all;
}