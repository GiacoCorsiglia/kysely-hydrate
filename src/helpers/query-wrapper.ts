import * as k from "kysely";

interface RootOperationNodeSource {
	toOperationNode: () => k.RootOperationNode;
	withPlugin: (plugin: k.KyselyPlugin) => this;
}

type Wrapper = (
	eb: k.ExpressionBuilder<any, any>,
	query: k.ExpressionWrapper<any, any, any>,
) => RootOperationNodeSource;

// This is so stupid but there's no way to get the "executor" from the select
// query builder so we gotta do this shit.
class WrapperPlugin implements k.KyselyPlugin {
	readonly #wrapper: Wrapper;

	constructor(wrapper: Wrapper) {
		this.#wrapper = wrapper;
	}

	transformQuery(args: k.PluginTransformQueryArgs): k.RootOperationNode {
		return this.#wrapper(
			k.expressionBuilder(),
			new k.ExpressionWrapper(args.node),
		).toOperationNode();
	}

	async transformResult(args: k.PluginTransformResultArgs): Promise<k.QueryResult<k.UnknownRow>> {
		return args.result;
	}
}

export function wrapQuery<T extends RootOperationNodeSource>(query: T, wrapper: Wrapper): T {
	return query.withPlugin(new WrapperPlugin(wrapper));
}
