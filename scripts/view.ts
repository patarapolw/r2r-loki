import R2r from "../src";

(async () => {
  const r2r = await new R2r("test.r2r").build();
  console.log(r2r.card.find().slice(0, 10));
  console.log(r2r.note.find().slice(0, 10));
  console.log(r2r.template.find().slice(0, 10));
  console.log(r2r.source.find().slice(0, 10));
  console.log(r2r.deck.find().slice(0, 10));
  await r2r.close();
})().catch(console.error);
