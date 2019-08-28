export function ankiMustache(s: string, d: Record<string, any> = {}, front: string = ""): string {
  s = s.replace(/{{FrontSide}}/g, front.replace(/@html\n/g, ""))

  for (const [k, v] of Object.entries(d)) {
      if (typeof v === "string") {
          s = s.replace(
              new RegExp(`{{(\\S+:)?${escapeRegExp(k)}}}`, "g"),
              v.replace(/^@[^\n]+\n/gs, "")
          );
      }
  }

  s = s.replace(/{{#(\S+)}}([^]*){{\1}}/gs, (m, p1, p2) => {
      return Object.keys(d).includes(p1) ? p2 : "";
  });

  s = s.replace(/{{[^}]+}}/g, "");

  return s;
}

export function shuffle(a: any[]) {
  for (let i = a.length - 1; i > 0; i--) {
      const j = Math.floor(Math.random() * (i + 1));
      [a[i], a[j]] = [a[j], a[i]];
  }
  return a;
}

export function escapeRegExp(s: string) {
  return s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');  // $& means the whole matched string
}

export function chunk<T>(array: T[], size: number): T[][] {
  var results: T[][] = [],
    length = Math.ceil(array.length / size);

  for (var i = 0; i < length; i++) {
    results.push(array.slice(i * size, (i + 1) * size));
  }
  return results;
}