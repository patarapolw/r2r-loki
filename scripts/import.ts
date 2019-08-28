import R2r from "../src";
import Anki from "ankisync";

(async () => {
  const r2r = await R2r.connect("test.r2r");
  const anki = await Anki.connect("/Users/patarapolw/Downloads/Hanyu_Shuiping_Kaoshi_HSK_all_5000_words_high_quality.apkg");
  await r2r.fromAnki(anki, console.log);
  await anki.close();
  await r2r.close();
})().catch(console.error);
