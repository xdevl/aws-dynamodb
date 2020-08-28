import {DynamoSerializer} from "../lib/serializer";
import {Initializer} from "../lib/utils";

enum Gender {
    MALE = "MALE", FEMALE = "FEMALE"
}

class User {
    public readonly surname: string;
    public readonly firstNames: string[];
    public readonly dateOfBirth: Date;
    public readonly gender: Gender;
    public readonly mentor?: User;
    public readonly rightHanded: boolean;

    constructor(attrs: Initializer<User>) {
        this.surname = attrs.surname;
        this.firstNames = attrs.firstNames;
        this.dateOfBirth = attrs.dateOfBirth;
        this.gender = attrs.gender;
        this.mentor = attrs.mentor;
        this.rightHanded = attrs.rightHanded;
    }
}

const userSerializer = new DynamoSerializer<User>((define, self) => define({
    surname: DynamoSerializer.string(),
    firstNames: DynamoSerializer.list(DynamoSerializer.string()),
    dateOfBirth: DynamoSerializer.date(),
    gender: DynamoSerializer.enum(),
    mentor: DynamoSerializer.optional(self),
    rightHanded: DynamoSerializer.boolean(),
}), (attrs) => new User(attrs));

const johny = {
    user: new User({
        surname: "Johny",
        firstNames: ["Smith", "Jones"],
        dateOfBirth: new Date(123456789),
        gender: Gender.MALE,
        rightHanded: true,
    }),
    serialized: {
        M: {
            surname: {S: "Johny"},
            firstNames: {L: [{S: "Smith"}, {S: "Jones"}]},
            dateOfBirth: {N: "123456789"},
            gender: {S: "MALE"},
            mentor: {NULL: true},
            rightHanded: {BOOL: true},
        }
    }
};

const sarah = {
    user: new User({
        surname: "Sarah",
        firstNames: ["Roberts"],
        dateOfBirth: new Date(987654321),
        gender: Gender.FEMALE,
        mentor: johny.user,
        rightHanded: false
    }),
    serialized: {
        M: {
            surname: {S: "Sarah"},
            firstNames: {L: [{S: "Roberts"}]},
            dateOfBirth: {N: "987654321"},
            gender: {S: "FEMALE"},
            mentor: johny.serialized,
            rightHanded: {BOOL: false},
        }
    }
}

test("Can serialize", () => {
    expect(userSerializer.serialize(sarah.user)).toStrictEqual(sarah.serialized);
});

test("Can serialize", () => {
    expect(userSerializer.deserialize(sarah.serialized)).toStrictEqual(sarah.user);
});