/*global _, $, Backbone, console */
const Tools = {
  binarySearch: function(array, element, compareFn) {
    let mid = 0,
      n = array.length - 1,
      cmp,
      i;

    while (mid <= n) {
      i = (n + mid) >> 1;
      cmp = compareFn(element, array[i]);
      if (cmp > 0) {
        mid = i + 1;
      } else if (cmp < 0) {
        n = i - 1;
      } else {
        return i;
      }
    }

    // returns a negative value indicating the insertion point
    // for the new element if the element is not found
    return -mid - 1;
  },

  compareKDBTimes: function(a, b) {
    return a.i > b.i ? 1 : a.i < b.i ? -1 : a.n > b.n ? 1 : a.n < b.n ? -1 : 0;
  },

  uriToObj: function(uri) {
    return _.reduce(
      uri.split("&"),
      function(memo, item) {
        const kvp = item.split("=");
        memo[kvp[0]] = kvp[1];
        return memo;
      },
      {}
    );
  }
};
