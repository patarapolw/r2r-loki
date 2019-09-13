export function shuffle(a: any[]) {
  for (let i = a.length - 1; i > 0; i--) {
      const j = Math.floor(Math.random() * (i + 1));
      [a[i], a[j]] = [a[j], a[i]];
  }
  return a;
}

export function chunk<T>(array: T[], size: number): T[][] {
  var results: T[][] = [],
    length = Math.ceil(array.length / size);

  for (var i = 0; i < length; i++) {
    results.push(array.slice(i * size, (i + 1) * size));
  }
  return results;
}

declare global {
  interface Array<T> {
    distinctBy<U extends string | number>(mapFn: (el: T) => U): T[];
    mapAsync<U>(mapFn: (value: T, index: number, array: T[]) => Promise<U>): Promise<U[]>;
  }
}

Array.prototype.distinctBy = function(mapFn) {
  const uniqueKeys = new Set(this.map(mapFn));
  return this.filter((el) => uniqueKeys.has(mapFn(el)));
}

Array.prototype.mapAsync = async function(mapFn) {
  return await Promise.all(this.map(async (value, index, array) => {
    try {
      return await mapFn(value, index, array);
    } catch(e) {
      throw e;
    }
  }));
}