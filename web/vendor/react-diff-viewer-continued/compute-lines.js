import {
  DiffMethod,
  DiffType,
  computeDiff,
  computeLineInformation,
} from "./compute-core.js";

/**
 * Computes line diff information using a Web Worker to avoid blocking the UI thread.
 * This offloads the expensive `computeLineInformation` logic to a separate thread.
 *
 * @param oldString Old string to compare.
 * @param newString New string to compare with old string.
 * @param disableWordDiff Flag to enable/disable word diff.
 * @param lineCompareMethod JsDiff text diff method from https://github.com/kpdecker/jsdiff/tree/v4.0.1#api
 * @param linesOffset line number to start counting from
 * @param showLines lines that are always shown, regardless of diff
 * @returns Promise<ComputedLineInformation> - Resolves with line-by-line diff data from the worker.
 */
const computeLineInformationWorker = (
  oldString,
  newString,
  disableWordDiff = false,
  lineCompareMethod = DiffMethod.CHARS,
  linesOffset = 0,
  showLines = [],
  deferWordDiff = false
) => {
  // Fall back to synchronous computation if Worker is not available (e.g., in Node.js/test environments)
  if (typeof Worker === "undefined") {
    return Promise.resolve(
      computeLineInformation(
        oldString,
        newString,
        disableWordDiff,
        lineCompareMethod,
        linesOffset,
        showLines,
        deferWordDiff
      )
    );
  }
  return new Promise((resolve, reject) => {
    const worker = new Worker(new URL("./computeWorker.js", import.meta.url), {
      type: "module",
    });
    worker.onmessage = (e) => {
      resolve(e.data);
      worker.terminate();
    };
    worker.onerror = (err) => {
      reject(err);
      worker.terminate();
    };
    worker.postMessage({
      oldString,
      newString,
      disableWordDiff,
      lineCompareMethod,
      linesOffset,
      showLines,
      deferWordDiff,
    });
  });
};

export {
  DiffMethod,
  DiffType,
  computeDiff,
  computeLineInformation,
  computeLineInformationWorker,
};
