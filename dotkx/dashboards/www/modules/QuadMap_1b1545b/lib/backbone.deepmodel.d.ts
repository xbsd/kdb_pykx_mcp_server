import * as Backbone from "../../../../node_modules/@types/backbone";

declare module "../../../../node_modules/@types/backbone" {
    export class DeepModel extends Backbone.Model {
        toFlat(flattenArrays: boolean): any;
    }
}
