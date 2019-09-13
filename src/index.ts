import Loki, { Collection } from "@lokidb/loki";
import { FSStorage } from "@lokidb/fs-storage";
import fs from "fs";
import SparkMD5 from "spark-md5";
import { srsMap, getNextReview, repeatReview } from "./quiz";
import QParser from "q2filter";
import uuid from "uuid/v4";
import { shuffle, chunk } from "./util";
import stringify from "fast-json-stable-stringify";
import Anki, { IMedia } from "ankisync";
import { R2rLocal, fromSortedData, ICondOptions, IEntry, IPagedOutput, toSortedData, toDate, IRender, ankiMustache } from "@rep2recall/r2r-format";

FSStorage.register();

export interface IDbDeck {
  $loki?: number;
  name: string;  // unique
}

export interface IDbSource {
  $loki?: number;
  h: string;  // unique
  name: string;
  created: string | Date;
}

export interface IDbTemplate {
  $loki?: number;
  key?: string;  // unique
  name: string;
  sourceId: number;
  front: string;
  back?: string;
  css?: string;
  js?: string;
}

export interface IDbNote {
  $loki?: number;
  key?: string;  // unique
  name: string;
  sourceId?: number;
  data: Record<string, any>;
  order: Record<string, number>;
}

export interface IDbMedia {
  $loki?: number;
  h?: string;  // unique
  sourceId?: number;
  name: string;
  data: ArrayBuffer;
}

export interface IDbCard {
  _id: string;
  deckId: number;
  templateId?: number;
  noteId?: number;
  front: string;
  back?: string;
  mnemonic?: string;
  srsLevel?: number;
  nextReview?: string | Date;
  tag?: string[];
  created: string | Date;
  modified?: string | Date;
  stat?: {
    streak: { right: number; wrong: number };
  }
}

export default class R2rLoki extends R2rLocal {
  public loki: Loki;
  public deck!: Collection<IDbDeck>;
  public card!: Collection<IDbCard>;
  public source!: Collection<IDbSource>;
  public template!: Collection<IDbTemplate>;
  public note!: Collection<IDbNote>;
  public media!: Collection<IDbMedia>;

  constructor(filename: string) {
    super(filename);
    this.loki = new Loki(filename);
  }

  public async build() {
    await this.loki.initializePersistence({
      autoload: fs.existsSync(this.filename),
      autosave: true,
      autosaveInterval: 4000
    });

    this.deck = this.loki.getCollection("deck");
    if (this.deck === null) {
      this.deck = this.loki.addCollection("deck", {
        unique: ["name"]
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
        unique: ["key"]
      });
    }

    const tHook = (t: Partial<IDbTemplate>) => {
      if (t.front || t.back || t.css || t.js) {
        const t0 = this.template.findOne(t);
        if (t0) {
          t = Object.assign(t0, t);
        }
        t.key = this.getTemplateId(t as any);
      }
    };

    this.template.on("pre-insert", tHook);
    this.template.on("pre-update", tHook);

    this.note = this.loki.getCollection("note");
    if (this.note === null) {
      this.note = this.loki.addCollection("note", {
        unique: ["key"]
      });
    }

    const nHook = (n: IDbNote) => {
      if (Array.isArray(n.data)) {
        const { order, data } = fromSortedData(n.data as any[]);
        n.order = order;
        n.data = data;
      }

      if (!n.key && n.data) {
        n.key = this.getNoteId(n.data);
      }
    };

    this.note.on("pre-insert", nHook);
    this.note.on("pre-update", nHook);

    this.media = this.loki.getCollection("media");
    if (this.media === null) {
      this.media = this.loki.addCollection("media", {
        unique: ["h"]
      });
    }

    const mHook = (m: IDbMedia) => {
      if (!m.h && m.data) {
        m.h = SparkMD5.ArrayBuffer.hash(m.data);
      }
    };

    this.media.on("pre-insert", mHook);
    this.media.on("pre-update", mHook);

    return this;
  }

  public async close() {
    await this.loki.close();
    return this;
  }

  public async reset() {
    this.source.findAndRemove({});
    this.media.findAndRemove({});
    this.template.findAndRemove({});
    this.note.findAndRemove({});
    this.card.findAndRemove({});
    this.deck.findAndRemove({});

    return this;
  }

  public getTemplateId(t: { front: string, back?: string, css?: string, js?: string }) {
    const { front, back, css, js } = t;
    return SparkMD5.hash(stringify({ front, back, css, js }));
  }

  public getNoteId(data: Record<string, any>) {
    return SparkMD5.hash(stringify(data));
  }

  public async parseCond(
    q: string,
    options: ICondOptions<IEntry> = {}
  ): Promise<IPagedOutput<Partial<IEntry>>> {
    const parser = new QParser({
      anyOf: ["template", "front", "mnemonic", "deck", "tag"],
      isString: ["template", "front", "back", "mnemonic", "deck", "tag"],
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
    } else if (options.fields === "*") {
      options.fields = ["data", "source", "deck", "front", "js", "mnemonic", "modified",
        "nextReview", "sCreated", "sH", "srsLevel", "stat", "tBack", "tFront", "tag",
        "template", "back", "created", "css", "_id"];
    }

    const allFields = new Set<string>(options.fields || []);
    for (const f of (fullCond.fields || [])) {
      allFields.add(f);
    }

    if (q.includes("is:distinct") || q.includes("is:duplicate")) {
      allFields.add("key");
    }

    let query = this.card.chain();

    if (["data", "key"].some((k) => allFields.has(k))) {
      query = query.eqJoin(this.note, "noteId", "$loki", (l, r) => {
        l = cleanLokiObj(l);
        const { data, order } = r;
        return { ...l, data, order };
      });
    }

    if (["deck"].some((k) => allFields.has(k))) {
      query = query.eqJoin(this.deck, "deckId", "$loki", (l, r) => {
        l = cleanLokiObj(l);
        const { name } = r;
        return { ...l, deck: name };
      });
    }

    if (["sCreated", "sH", "source"].some((k) => allFields.has(k))) {
      query = query.eqJoin(this.source, "sourceId", "$loki", (l, r) => {
        l = cleanLokiObj(l);
        const { created, name } = r;
        return { ...l, sCreated: created, source: name };
      });
    }

    if (["tFront", "tBack", "template", "model", "css", "js"].some((k) => allFields.has(k))) {
      query = query.eqJoin(this.template, "templateId", "$loki", (l, r) => {
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
          if (options.fields.includes("data")) {
            c.data = toSortedData({ data: c.data, order: c.order });
          }

          for (const k of Object.keys(c)) {
            if (!options.fields.includes(k as any)) {
              delete (c as any)[k];
            }
          }
        }

        return c;
      }),
      count: cards.length
    };
  }

  public async insertMany(entries: IEntry[]): Promise<string[]> {
    entries = await entries.mapAsync((e) => this.transformCreateOrUpdate(null, e)) as IEntry[];

    const now = new Date();

    const sIdMap: Record<string, number> = {};
    entries.filter((e) => e.sH).distinctBy((e) => e.sH!).map((e) => {
      try {
        sIdMap[e.sH!] = this.source.insertOne({
          name: e.source!,
          created: (toDate(e.sCreated) || now).toISOString(),
          h: e.sH!
        }).$loki;
      } catch (err) { }
    });

    const tIdMap: Record<string, number> = {};
    entries.filter((e) => e.template).distinctBy((e) => e.template!).map((e) => {
      try {
        tIdMap[e.template!] = this.template.insertOne({
          name: e.template!,
          front: e.tFront!,
          back: e.tBack,
          css: e.css,
          js: e.js,
          sourceId: sIdMap[e.sH!]
        }).$loki;
      } catch (err) { }
    });

    const nIdMap: Record<string, number> = {};
    entries.filter((e) => e.data).map((e) => {
      const {order, data} = fromSortedData(e.data!);
      const key = SparkMD5.hash(stringify(data));
      (e as any).key = key;
      
      try {
        nIdMap[key] = this.note.insertOne({
          name: `${e.sH!}/${e.template}/${e.data![0].value}`,
          key,
          data,
          order,
          sourceId: sIdMap[e.sH!]
        }).$loki;
      } catch (e) { }
    });

    const dMap: { [key: string]: number } = {};
    entries.distinctBy((e) => e.deck).mapAsync(async (e) => {
      dMap[e.deck] = await this.getOrCreateDeck(e.deck);
    });

    const cIds: string[] = [];
    this.card.insert(entries.map((e) => {
      const _id = uuid();
      cIds.push(_id);
      return {
        _id,
        front: e.front,
        back: e.back,
        mnemonic: e.mnemonic,
        srsLevel: e.srsLevel,
        nextReview: toDate(e.nextReview),
        deckId: dMap[e.deck],
        noteId: (e as any).key ? nIdMap[(e as any).key] : undefined,
        templateId: e.template ? tIdMap[e.template] : undefined,
        created: now,
        tag: e.tag
      }
    }));

    return cIds;
  }

  public async updateMany(ids: string[], u: Partial<IEntry>) {
    const now = new Date();

    const cs = await this.card.find({ _id: { $in: ids } }).mapAsync(async (c) => {
      for (const k of Object.keys(c)) {
        if (!Object.keys(u).includes(k) && k !== "_id") {
          delete (c as any)[k];
        }
      }

      const c0: any = Object.assign(c, this.transformCreateOrUpdate(c._id!, u, now));
      const c1: any = { _id: c._id! };

      for (let [k, v] of Object.entries(c0)) {
        switch (k) {
          case "deck":
            k = "deckId";
            v = await this.getOrCreateDeck(v as string);
            c1[k] = v;
            break;
          case "tFront":
          case "tBack":
            k = k.substr(1).toLocaleLowerCase();
          case "css":
          case "js":
            const templateId = this.card.findOne({ _id: c._id! }).templateId;
            this.template.findAndUpdate({ $loki: templateId }, (t) => {
              (t as any)[k] = v;
              return t;
            });
            break;
          case "data":
            const noteId = this.card.findOne({ _id: c._id! }).noteId;
            const n = this.note.findOne({ $loki: noteId });
            if (n) {
              const { order, data } = n;
              for (const { key, value } of v as any[]) {
                if (!order![key]) {
                  order![key] = Math.max(...Object.values(order!)) + 1;
                }
                data![key] = value;
              }
              this.note.findAndUpdate({ $loki: noteId }, (n) => {
                return Object.assign(n, { order, data });
              });
            } else {
              const order: Record<string, number> = {};
              const data: Record<string, any> = {};
              for (const { key, value } of v as any[]) {
                if (!order[key]) {
                  order[key] = Math.max(-1, ...Object.values(order)) + 1;
                }
                data[key] = value;
              }

              const key = this.getNoteId(data)
              const name = `${key}/${Object.values(data)[0]}`;
              this.note.insertOne({ key, name, order, data });
              c1.noteId = key;
            }
            break;
          default:
            c1[k] = v;
        }
      }

      return c1;
    });

    for (const c of cs) {
      if (Object.keys(c).length > 1) {
        this.card.findAndUpdate({ _id: c._id }, (c0) => {
          return Object.assign(c0, c);
        });
      }
    }
  }

  public async addTags(ids: string[], tags: string[]) {
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

  public async removeTags(ids: string[], tags: string[]) {
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

  public async deleteMany(ids: string[]) {
    return this.card.removeWhere((c0) => ids.includes(c0._id));
  }

  public async render(cardId: string) {
    const r = await this.parseCond(`_id=${cardId}`, {
      limit: 1,
      fields: ["front", "back", "mnemonic", "tFront", "tBack", "data", "css", "js"]
    });

    const c = r.data[0] as IRender;
    const { tFront, tBack, data } = c;

    if (/@md5\n/.test(c.front)) {
      c.front = ankiMustache(tFront || "", data);
    }

    if (c.back && /@md5\n/.test(c.back)) {
      c.back = ankiMustache(tBack || "", data, c.front);
    }

    return c;
  }

  protected async updateSrsLevel(dSrsLevel: number, cardId: string) {
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
    await this.updateMany([cardId], { srsLevel, stat, nextReview });
  }

  protected async transformCreateOrUpdate(
    cardId: string | null,
    u: Partial<IEntry>,
    timestamp: Date = new Date()
  ): Promise<Partial<IEntry>> {
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
          data = await this.getData(cardId);
        } else {
          data = u.data || [];
        }
      }

      u.tBack = (u.front || "").substr("@template\n".length);
      if (!front && cardId) {
        front = await this.getFront(cardId);
      }
    }

    if (u.tBack) {
      const back = ankiMustache(u.tBack, data || {}, front);
      u.back = "@md5\n" + SparkMD5.hash(back);
    }

    return u;
  }

  protected async getOrCreateDeck(name: string): Promise<number> {
    try {
      return this.deck.insertOne({ name }).$loki;
    } catch (e) {
      return this.deck.findOne({ name }).$loki;
    }
  }

  protected async getData(cardId: string): Promise<Array<{key: string, value: any}> | null> {
    const c = this.card.findOne({ _id: cardId });
    if (c && c.noteId) {
      const n = this.note.findOne({ $loki: c.noteId });
      if (n) {
        const {order, data} = n.data;
        return toSortedData({order, data});
      }
    }

    return null;
  }

  protected async getFront(cardId: string): Promise<string> {
    const c = this.card.findOne({ _id: cardId });
    if (c) {
      if (c.front.startsWith("@md5\n") && c.templateId) {
        const t = this.template.findOne({ $loki: c.templateId });
        const data = this.getData(cardId);
        if (t) {
          return ankiMustache(t.front, data || {});
        }
      }

      return c.front;
    }

    return "";
  }

  public async getMedia(h: string): Promise<IMedia | null> {
    const m = this.media.findOne({ h }) as IMedia;
    return m || null;
  }

  public async allMedia() {
    return this.media.find({}) as IMedia[];
  }

  public async createMedia(m: {name: string, data: ArrayBuffer}) {
    const h = SparkMD5.ArrayBuffer.hash(m.data);
    try {
      this.media.insertOne({...m, h});
      return h;
    } catch(e) {};

    return "";
  }

  public async deleteMedia(h: string) {
    this.media.removeWhere((m) => m.h === h);
    return true;
  }

  public async export(r2r: R2rLocal, q: string = "", 
    options?: { callback?: (p: IProgress) => void }
  ) {
    const callback = options ? options.callback : undefined;
    let current = 1;

    const ms = await this.media.find({});
    for (const m of ms) {
      if (callback) callback({text: "Inserting media", current, max: ms.length});
      try {
        await r2r.createMedia(m as IMedia);
      } catch(e) {}
      current++;
    }
    
    if (callback) callback({text: "Parsing q"})
    const cs = await this.parseCond(q, {
      fields: "*"
    });

    current = 1;
    for (const c of chunk(cs.data as IEntry[], 1000)) {
      if (callback) callback({text: "Inserting cards", current, max: cs.count});
      await r2r.insertMany(c);
      current += 1000;
    }

    await r2r.close();
  }

  public async fromR2r(r2r: R2rLocal, options?: { filename?: string, callback?: (p: IProgress) => void }) {
    const filename = options ? options.filename : undefined;
    const callback = options ? options.callback : undefined;

    if (callback) callback({ text: "Reading R2r file" });

    const data = fs.readFileSync(r2r.filename);
    const sourceH = SparkMD5.ArrayBuffer.hash(data);
    const now = new Date().toISOString();
    let sourceId: number;

    try {
      sourceId = this.source.insertOne({
        name: filename || r2r.filename,
        h: sourceH,
        created: now
      }).$loki;
    } catch (e) {
      if (callback) callback({ text: "Duplicated Anki resource" });
      return;
    }

    (await r2r.allMedia()).map((m) => {
      try {
        this.media.insertOne({
          ...m,
          sourceId
        });
      } catch (e) { }
    });

    await this.insertMany((await r2r.parseCond("", {
      fields: "*"
    })).data as IEntry[]);
  }

  public async fromAnki(anki: Anki, options?: { filename?: string, callback?: (p: IProgress) => void }) {
    const filename = options ? options.filename : undefined;
    const callback = options ? options.callback : undefined;

    if (callback) callback({ text: "Reading Anki file" });

    const data = fs.readFileSync(anki.filePath);
    const sourceH = SparkMD5.ArrayBuffer.hash(data);
    const now = new Date().toISOString();
    let sourceId: number;

    try {
      sourceId = this.source.insertOne({
        name: filename || anki.filePath,
        h: sourceH,
        created: now
      }).$loki;
    } catch (e) {
      if (callback) callback({ text: "Duplicated Anki resource" });
      return;
    }

    let current: number;
    let max: number;

    const media = await anki.apkg.tables.media.all();
    current = 0;
    max = media.length;
    for (const el of media) {
      if (callback) callback({ text: "Inserting media", current, max });

      try {
        this.media.insertOne({
          h: el.h,
          name: el.name,
          data: el.data,
          sourceId
        })
      } catch (e) { }

      current++;
    }

    const card = await anki.apkg.tables.cards.all();
    const dIdMap: Record<string, number> = {};
    const tIdMap: Record<string, number> = {};
    const nIdMap: Record<string, number> = {};

    current = 0;
    max = card.length;

    for (const c of chunk(card, 1000)) {
      if (callback) callback({ text: "Inserting cards", current, max });

      for (const el of c) {
        if (!Object.keys(dIdMap).includes(el.deck.name)) {
          const name = el.deck.name;
          try {
            dIdMap[name] = this.deck.insertOne({ name }).$loki;
          } catch (e) {
            dIdMap[name] = this.deck.findOne({ name }).$loki;
          }
        }

        const t = {
          name: `${sourceH}/${el.note.model.name}/${el.template.name}`,
          sourceId,
          front: el.template.qfmt,
          back: el.template.afmt,
          css: el.note.model.css
        };
        const tKey = this.getTemplateId(t);
        if (!Object.keys(tIdMap).includes(tKey)) {
          try {
            tIdMap[tKey] = this.template.insertOne(t).$loki;
          } catch (e) { }
        }

        const data: Record<string, string> = {};
        const order: Record<string, number> = {};
        el.template.model.flds.forEach((k, i) => {
          data[k] = el.note.flds[i];
          order[k] = i;
        });
        const key = SparkMD5.hash(stringify(data));
        if (!Object.keys(nIdMap).includes(key)) {
          try {
            nIdMap[key] = this.note.insertOne({
              key,
              name: `${sourceH}/${el.note.model.name}/${el.template.name}/${el.note.flds[0]}`,
              data,
              order
            }).$loki;
          } catch (e) { }
        }

        try {
          const front = ankiMustache(el.template.qfmt, data);
          const back = ankiMustache(el.template.afmt, data, front);

          this.card.insertOne({
            _id: uuid(),
            deckId: dIdMap[el.deck.name],
            templateId: tIdMap[tKey],
            noteId: nIdMap[key],
            front: `@md5\n${SparkMD5.hash(front)}`,
            back: `@md5\n${SparkMD5.hash(back)}`,
            created: now,
            tag: el.note.tags.filter((t) => t)
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