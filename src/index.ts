import Loki, { Collection } from "@lokidb/loki";
import { FSStorage } from "@lokidb/fs-storage";
import fs from "fs";
import SparkMD5 from "spark-md5";
import { srsMap, getNextReview, repeatReview } from "./quiz";
import QParser from "q2filter";
import uuid from "uuid/v4";
import shortid from "shortid";
import { shuffle, ankiMustache, chunk } from "./util";
import stringify from "es6-json-stable-stringify";
import Anki from "ankisync";

FSStorage.register();

export interface IDbDeck {
    _id: string;
    name: string;
}

export interface IDbSource {
    h: string;  // h as _id
    name: string;
    created: string;
}

export interface IDbTemplate {
    name: string;  // name as _id
    sourceId: string;
    front: string;
    back?: string;
    css?: string;
    js?: string;
}

export interface IDbNote {
    key: string;  // key as _id
    sourceId?: string;
    data: Record<string, any>;
    order: Record<string, number>;
}

export interface IDbMedia {
    h: string;  // h as _id
    sourceId?: string;
    name: string;
    data: Buffer;
}

export interface IDbCard {
    _id: string;
    deckId: string;
    templateId?: string;
    noteId?: string;
    front: string;
    back?: string;
    mnemonic?: string;
    srsLevel?: number;
    nextReview?: string;
    tag?: string[];
    created: string;
    modified?: string;
    stat?: {
        streak: { right: number; wrong: number };
    }
}

export interface IEntry {
    front: string;
    deck: string;
    back?: string;
    mnemonic?: string;
    srsLevel?: number;
    nextReview?: string;
    tag?: string[];
    created?: string;
    modified?: string;
    stat?: {
        streak: { right: number; wrong: number };
    };
    template?: string;
    tFront?: string;
    tBack?: string;
    css?: string;
    js?: string;
    key?: string;
    data?: { key: string, value: any }[];
    source?: string;
    sH?: string;
    sCreated?: string;
}

interface ICondOptions {
    offset?: number;
    limit?: number;
    sortBy?: string;
    desc?: boolean;
    fields?: string[];
}

interface IPagedOutput<T> {
    data: T[];
    count: number;
}

export default class R2r {
    public static async connect(filename: string) {
        const loki = new Loki(filename);
        await loki.initializePersistence({
            autoload: fs.existsSync(filename),
            autosave: true,
            autosaveInterval: 4000
        });

        return new R2r(loki);
    }

    public loki: Loki;
    public deck: Collection<IDbDeck>;
    public card: Collection<IDbCard>;
    public source: Collection<IDbSource>;
    public template: Collection<IDbTemplate>;
    public note: Collection<IDbNote>;
    public media: Collection<IDbMedia>;

    private constructor(loki: Loki) {
        this.loki = loki;

        this.deck = this.loki.getCollection("deck");
        if (this.deck === null) {
            this.deck = this.loki.addCollection("deck", {
                unique: ["name", "_id"]
            });
        }

        this.card = this.loki.getCollection("card");
        if (this.card === null) {
            this.card = this.loki.addCollection("card", {
                unique: ["_id"]
            });
        }

        this.source = this.loki.getCollection("source");
        if (this.source === null) {
            this.source = this.loki.addCollection("source", {
                unique: ["h"]
            });
        }

        this.template = this.loki.getCollection("template");
        if (this.template === null) {
            this.template = this.loki.addCollection("template", {
                unique: ["name"]
            });
        }

        this.note = this.loki.getCollection("note");
        if (this.note === null) {
            this.note = this.loki.addCollection("note", {
                unique: ["key"]
            });
        }

        this.media = this.loki.getCollection("media");
        if (this.media === null) {
            this.media = this.loki.addCollection("media", {
                unique: ["h"]
            });
        }
    }

    public async close() {
        await this.loki.close();
    }

    public parseCond(
        q: string,
        options: ICondOptions = {}
    ): IPagedOutput<any> {
        const parser = new QParser({
            anyOf: ["template", "front", "mnemonic", "entry", "deck", "tag"],
            isString: ["template", "front", "back", "mnemonic", "deck", "tag", "entry"],
            isDate: ["created", "modified", "nextReview"],
            transforms: {
                "is:due": () => {
                    return { nextReview: { $lt: new Date() } }
                }
            },
            filters: {
                "is:distinct": (items: any[]) => {
                    const col: Record<string, any> = {};
                    for (const it of items) {
                        const k = it.key;
                        if (k) {
                            if (!col[k]) {
                                col[k] = it;
                            }
                        } else {
                            col[uuid()] = it;
                        }
                    }
                    return Object.values(col);
                },
                "is:duplicate": (items: any[]) => {
                    const col: Record<string, any[]> = {};
                    for (const it of items) {
                        const k = it.front;
                        col[k] = col[k] || [];
                        col[k].push(it);
                    }
                    return Object.values(col).filter((a) => a.length > 1).reduce((a, b) => [...a, ...b], []);
                },
                "is:random": (items: any[]) => {
                    return shuffle(items);
                }
            },
            sortBy: options.sortBy,
            desc: options.desc
        });

        const fullCond = parser.getCondFull(q);

        if (!options.fields) {
            return {
                data: [],
                count: 0
            };
        }

        const allFields = new Set(options.fields || []);
        for (const f of (fullCond.fields || [])) {
            allFields.add(f);
        }

        if (q.includes("is:distinct") || q.includes("is:duplicate")) {
            allFields.add("key");
        }

        let query = this.card.chain();

        if (["data", "key"].some((k) => allFields.has(k))) {
            query = query.eqJoin(this.note, "noteId", "key", (l, r) => {
                l = cleanLokiObj(l);
                const { data } = r;
                return { ...l, data };
            });
        }

        if (["deck"].some((k) => allFields.has(k))) {
            query = query.eqJoin(this.deck, "deckId", "_id", (l, r) => {
                l = cleanLokiObj(l);
                const { name } = r;
                return { ...l, deck: name };
            });
        }

        if (["sCreated", "sH", "source"].some((k) => allFields.has(k))) {
            query = query.eqJoin(this.source, "sourceId", "h", (l, r) => {
                l = cleanLokiObj(l);
                const { created, name } = r;
                return { ...l, sCreated: created, source: name };
            });
        }

        if (["tFront", "tBack", "template", "model", "css", "js"].some((k) => allFields.has(k))) {
            query = query.eqJoin(this.template, "templateId", "name", (l, r) => {
                l = cleanLokiObj(l);
                const { front, back, css, js } = r;
                return { ...l, tFront: front, tBack: back, css, js };
            });
        }
        let cards = parser.filter(query.data(), q);

        let endPoint: number | undefined;
        if (options.limit) {
            endPoint = (options.offset || 0) + options.limit;
        }

        return {
            data: cards.slice(options.offset || 0, endPoint).map((c: any) => {
                if (options.fields) {
                    for (const k of Object.keys(c)) {
                        if (!options.fields.includes(k)) {
                            delete (c as any)[k];
                        }
                    }
                }

                return c;
            }),
            count: cards.length
        };
    }

    public insertMany(entries: IEntry[]): string[] {
        entries = entries.map((e) => this.transformCreateOrUpdate(null, e)) as IEntry[];

        const eValidSource = entries.filter((e) => e.sH);
        const now = new Date().toISOString();

        let sourceH: string = "";
        for (const e of eValidSource.filter((e, i) => {
            return eValidSource.map((e1) => e1.sH).indexOf(e.sH) === i
        })) {
            sourceH = e.sH!;
            try {
                this.source.insertOne({
                    name: e.source!,
                    created: e.sCreated || now,
                    h: e.sH!
                })
            } catch (err) { }
        }

        const eValidTemplate = entries.filter((e) => e.tFront);

        eValidTemplate.map((e) => {
            try {
                this.template.insertOne({
                    name: e.template!,
                    front: e.tFront!,
                    back: e.tBack,
                    css: e.css,
                    js: e.js,
                    sourceId: sourceH
                });
            } catch (e) { }
        })

        const eValidNote = entries.filter((e) => e.data);

        eValidNote.map((e) => {
            const data: Record<string, any> = {};
            const order: Record<string, number> = {};

            let index = 1;
            for (const { key, value } of e.data!) {
                data[key] = value;
                order[key] = index
                index++;
            }

            try {
                this.note.insertOne({
                    key: e.key!,
                    data,
                    order,
                    sourceId: sourceH
                });
            } catch (e) { }
        })

        const dMap: { [key: string]: string } = {};
        const decks = entries.map((e) => e.deck);
        const deckIds = decks.map((d) => this.getOrCreateDeck(d));
        decks.forEach((d, i) => {
            dMap[d] = deckIds[i];
        });

        const cIds: string[] = [];
        this.card.insert(entries.map((e) => {
            const _id = shortid.generate();
            cIds.push(_id);
            return {
                _id,
                front: e.front,
                back: e.back,
                mnemonic: e.mnemonic,
                srsLevel: e.srsLevel,
                nextReview: e.nextReview,
                deckId: dMap[e.deck],
                noteId: e.key,
                templateId: e.template,
                created: now,
                tag: e.tag
            }
        }));

        return cIds;
    }

    public updateMany(ids: string[], u: Partial<IEntry>) {
        const now = new Date().toISOString();

        return this.card.updateWhere((c0) => ids.includes(c0._id), (c0) => {
            return Object.assign(c0, this.transformCreateOrUpdate(c0._id, u, now));
        });
    }

    public addTags(ids: string[], tags: string[]) {
        const now = new Date().toISOString();

        return this.card.updateWhere((c0) => ids.includes(c0._id), (c0) => {
            c0.modified = now;
            c0.tag = c0.tag || [];
            for (const t of tags) {
                if (!c0.tag.includes(t)) {
                    c0.tag.push(t);
                }
            }
            return c0;
        });
    }

    public removeTags(ids: string[], tags: string[]) {
        const now = new Date().toISOString();

        return this.card.updateWhere((c0) => ids.includes(c0._id), (c0) => {
            c0.modified = now;
            const newTags: string[] = [];

            for (const t of (c0.tag || [])) {
                if (!tags.includes(t)) {
                    newTags.push(t);
                }
            }
            if (newTags.length > 0) {
                c0.tag = newTags;
            } else {
                delete c0.tag;
            }

            return c0;
        });
    }

    public deleteMany(ids: string[]) {
        return this.card.removeWhere((c0) => ids.includes(c0._id));
    }

    public render(cardId: string) {
        const r = this.parseCond(`_id=${cardId}`, {
            limit: 1,
            fields: ["front", "back", "mnemonic", "tFront", "tBack", "data", "css", "js"]
        });

        const c = r.data[0];
        const { tFront, tBack, data } = c;

        if (/@md5\n/.test(c.front)) {
            c.front = ankiMustache(tFront || "", data);
        }

        if (c.back && /@md5\n/.test(c.back)) {
            c.back = ankiMustache(tBack || "", data, c.front);
        }

        return c;
    }

    public markRight(cardId: string) {
        return this.updateSrsLevel(+1, cardId);
    }

    public markWrong(cardId: string) {
        return this.updateSrsLevel(-1, cardId);
    }

    private updateSrsLevel(dSrsLevel: number, cardId: string) {
        const card = this.card.findOne({ _id: cardId });

        if (!card) {
            return;
        }

        card.srsLevel = card.srsLevel || 0;
        card.stat = card.stat || {
            streak: {
                right: 0,
                wrong: 0
            }
        };
        card.stat.streak = card.stat.streak || {
            right: 0,
            wrong: 0
        }

        if (dSrsLevel > 0) {
            card.stat.streak.right = (card.stat.streak.right || 0) + 1;
        } else if (dSrsLevel < 0) {
            card.stat.streak.wrong = (card.stat.streak.wrong || 0) + 1;
        }

        card.srsLevel += dSrsLevel;

        if (card.srsLevel >= srsMap.length) {
            card.srsLevel = srsMap.length - 1;
        }

        if (card.srsLevel < 0) {
            card.srsLevel = 0;
        }

        if (dSrsLevel > 0) {
            card.nextReview = getNextReview(card.srsLevel).toISOString();
        } else {
            card.nextReview = repeatReview().toISOString();
        }

        const { srsLevel, stat, nextReview } = card;
        this.updateMany([cardId], { srsLevel, stat, nextReview });
    }

    private transformCreateOrUpdate(
        cardId: string | null,
        u: Partial<IEntry>,
        timestamp: string = new Date().toISOString()
    ): Partial<IEntry> {
        let data: Record<string, any> | null = null;
        let front: string = "";

        if (!cardId) {
            u.created = timestamp;
        } else {
            u.modified = timestamp;
        }

        if (u.front && u.front.startsWith("@template\n")) {
            if (!data) {
                if (cardId) {
                    data = this.getData(cardId);
                } else {
                    data = u.data || [];
                }
            }

            u.tFront = u.front.substr("@template\n".length);
        }

        if (u.tFront) {
            front = ankiMustache(u.tFront, data || {});
            u.front = "@md5\n" + SparkMD5.hash(front);
        }

        if (u.back && u.back.startsWith("@template\n")) {
            if (!data) {
                if (cardId) {
                    data = this.getData(cardId);
                } else {
                    data = u.data || [];
                }
            }

            u.tBack = (u.front || "").substr("@template\n".length);
            if (!front && cardId) {
                front = this.getFront(cardId);
            }
        }

        if (u.tBack) {
            const back = ankiMustache(u.tBack, data || {}, front);
            u.back = "@md5\n" + SparkMD5.hash(back);
        }

        return u;
    }

    private getOrCreateDeck(name: string): string {
        try {
            const _id = shortid.generate();
            this.deck.insertOne({ _id, name });
            return _id;
        } catch (e) {
            return this.deck.findOne({ name })._id;
        }
    }

    private getData(cardId: string): Record<string, any> | null {
        const c = this.card.findOne({ _id: cardId });
        if (c && c.noteId) {
            const n = this.note.findOne({ key: c.noteId });
            if (n) {
                return n.data;
            }
        }

        return null;
    }

    private getFront(cardId: string): string {
        const c = this.card.findOne({ _id: cardId });
        if (c) {
            if (c.front.startsWith("@md5\n") && c.templateId) {
                const t = this.template.findOne({ name: c.templateId });
                const data = this.getData(cardId);
                if (t) {
                    return ankiMustache(t.front, data || {});
                }
            }

            return c.front;
        }

        return "";
    }

    public async fromAnki(anki: Anki, callback?: (p: IProgress) => void) {
        if (callback) callback({text: "Reading Anki file"});

        const data = fs.readFileSync(anki.filePath);
        const sourceH = SparkMD5.ArrayBuffer.hash(data);
        const now = new Date().toISOString();

        try {
            this.source.insertOne({
                name: anki.filePath,
                h: sourceH,
                created: now
            });
        } catch(e) {
            if (callback) callback({text: "Duplicated Anki resource"});
            return;
        }

        let current: number;
        let max: number;

        const media = await anki.apkg.tables.media.all();
        current = 0;
        max = media.length;
        for (const el of media) {
            if (callback) callback({text: "Inserting media", current, max});

            try {
                this.media.insertOne({
                    h: el.h,
                    name: el.name,
                    data: el.data,
                    sourceId: sourceH
                })
            } catch(e) {}

            current++;
        }

        const card = await anki.apkg.tables.cards.all();
        const deckIdMap: Record<string, string> = {};
        const templateIdSet = new Set<string>();
        const noteIdSet = new Set<string>();

        current = 0;
        max = card.length;

        for (const c of chunk(card, 1000)) {
            if (callback) callback({text: "Inserting cards", current, max});

            for (const el of c) {
                if (!Object.keys(deckIdMap).includes(el.deck.name)) {
                    const name = el.deck.name;
                    try {
                        const _id = uuid();
                        this.deck.insertOne({ _id, name });
                        deckIdMap[name] = _id;
                    } catch (e) {
                        const d = this.deck.findOne({ name });
                        deckIdMap[name] = d._id;
                    }
                }

                const templateId = `${sourceH}/${el.template.model.name}/${el.template.name}`;
                if (!templateIdSet.has(templateId)) {
                    templateIdSet.add(templateId);
                    try {
                        this.template.insertOne({
                            name: templateId,
                            sourceId: sourceH,
                            front: el.template.qfmt,
                            back: el.template.afmt
                        });
                    } catch (e) { }
                }

                const data: Record<string, string> = {};
                const order: Record<string, number> = {};
                el.template.model.flds.forEach((k, i) => {
                    data[k] = el.note.flds[i];
                    order[k] = i;
                });
                const key = `${sourceH}/${SparkMD5.hash(stringify(data))}`;
                if (!noteIdSet.has(key)) {
                    try {
                        this.note.insertOne({
                            key,
                            data,
                            order
                        });
                    } catch (e) { }
                }

                try {
                    const front = ankiMustache(el.template.qfmt, data);
                    const back = ankiMustache(el.template.afmt, data, front);

                    this.card.insertOne({
                        _id: uuid(),
                        deckId: deckIdMap[el.deck.name],
                        templateId,
                        noteId: key,
                        front: `@md5\n${SparkMD5.hash(front)}`,
                        back: `@md5\n${SparkMD5.hash(back)}`,
                        created: now
                    });
                } catch (e) { }
            }

            current += 1000;
        }
    }
}

export function cleanLokiObj(el: any) {
    delete el.$loki;
    delete el.meta;
    return el;
}

interface IProgress {
    text: string;
    current?: number;
    max?: number;
}