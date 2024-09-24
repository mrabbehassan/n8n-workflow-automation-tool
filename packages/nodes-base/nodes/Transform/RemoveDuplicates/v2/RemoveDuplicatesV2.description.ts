import type { INodeProperties } from 'n8n-workflow';
const operationOptions = [
	{
		name: 'Remove Items Repeated Within Current Input',
		value: 'removeDuplicateInputItems',
		description: 'Remove duplicates from incoming items',
		action: 'Remove items repeated within current input',
	},
	{
		name: 'Remove Items Processed in Previous Executions',
		value: 'removeItemsSeenInPreviousExecutions',
		description: 'Deduplicate items already seen in previous executions',
		action: 'Remove items processed in previous executions',
	},
	{
		name: 'Clear Deduplication History',
		value: 'clearDeduplicationHistory',
		description: 'Wipe the store of previous items',
		action: 'Clear deduplication history',
	},
];
const compareOptions = [
	{
		name: 'All Fields',
		value: 'allFields',
	},
	{
		name: 'All Fields Except',
		value: 'allFieldsExcept',
	},
	{
		name: 'Selected Fields',
		value: 'selectedFields',
	},
];
const logicOptions = [
	{
		name: 'Field Value Is New',
		value: 'removeItemsWithAlreadySeenKeyValues',
		description: 'Remove all input items with key values matching those already processed',
	},
	{
		name: 'Field Value Is Higher than Any Previous Value',
		value: 'removeItemsUpToStoredIncrementalKey',
		description:
			'Works with incremental key values, removes all input items with key values up to the stored value',
	},
	{
		name: 'Field Value Is a Date Later than Any Previous Date',
		value: 'RemoveItemsUpToStoredDate',
		description:
			'Works with date key values, removes all input items with key values up to the stored date',
	},
];
const manageDatabaseModeOptions = [
	{
		name: 'Clean Database',
		value: 'cleanDatabase',
		description: 'Clear all values stored for a key in the database',
	},
];

export const removeDuplicatesNodeFields: INodeProperties[] = [
	{
		displayName: 'Operation',
		name: 'operation',
		type: 'options',
		noDataExpression: true,
		options: operationOptions,
		default: 'removeDuplicateInputItems',
	},
	{
		displayName: 'Compare',
		name: 'compare',
		type: 'options',
		options: compareOptions,
		default: 'allFields',
		description: 'The fields of the input items to compare to see if they are the same',
		displayOptions: {
			show: {
				operation: ['removeDuplicateInputItems'],
			},
		},
	},
	{
		displayName: 'Fields To Exclude',
		name: 'fieldsToExclude',
		type: 'string',
		placeholder: 'e.g. email, name',
		requiresDataPath: 'multiple',
		description: 'Fields in the input to exclude from the comparison',
		default: '',
		displayOptions: {
			show: {
				compare: ['allFieldsExcept'],
			},
		},
	},
	{
		displayName: 'Fields To Compare',
		name: 'fieldsToCompare',
		type: 'string',
		placeholder: 'e.g. email, name',
		requiresDataPath: 'multiple',
		description: 'Fields in the input to add to the comparison',
		default: '',
		displayOptions: {
			show: {
				compare: ['selectedFields'],
			},
		},
	},

	// ----------------------------------
	{
		displayName: 'Keep items where',
		name: 'logic',
		type: 'options',
		noDataExpression: true,
		options: logicOptions,
		default: 'removeItemsWithAlreadySeenKeyValues',
		description:
			'How to select input items to remove by comparing them with key values previously processed',
		displayOptions: {
			show: {
				operation: ['removeItemsSeenInPreviousExecutions'],
			},
		},
	},
	{
		displayName: 'Dedupe Field',
		name: 'dedupeField',
		type: 'string',
		default: '',
		description: 'Use an input field (or a combination of fields) that has a unique ID value',
		hint: 'The input field value to compare between items',
		placeholder: 'e.g. ID',
		displayOptions: {
			show: {
				logic: ['removeItemsWithAlreadySeenKeyValues'],
			},
		},
	},
	{
		displayName: 'Dedupe Field',
		name: 'incrementalDedupeField',
		type: 'number',
		default: '',
		description: 'Use an input field (or a combination of fields) that has an incremental value',
		hint: 'The input field value to compare between items, an incremental value is expected',
		placeholder: 'e.g. ID',
		displayOptions: {
			show: {
				logic: ['removeItemsUpToStoredIncrementalKey'],
			},
		},
	},
	{
		displayName: 'Dedupe Field',
		name: 'dateDedupeField',
		type: 'dateTime',
		default: '',
		description: 'Use an input field that has a date value in ISO format',
		hint: 'The input field value to compare between items, a date is expected',
		placeholder: ' e.g. 2024-08-09T13:44:16Z',
		displayOptions: {
			show: {
				logic: ['RemoveItemsUpToStoredDate'],
			},
		},
	},
	{
		displayName: 'Mode',
		name: 'mode',
		type: 'options',
		default: 'cleanDatabase',
		description:
			'How you want to modify the key values stored on the database. None of these modes removes input items.',
		displayOptions: {
			show: {
				operation: ['clearDeduplicationHistory'],
			},
		},
		options: manageDatabaseModeOptions,
	},
	{
		displayName: 'Options',
		name: 'options',
		type: 'collection',
		placeholder: 'Add Field',
		default: {},
		displayOptions: {
			show: {
				operation: [
					'removeDuplicateInputItems',
					'removeItemsSeenInPreviousExecutions',
					'clearDeduplicationHistory',
				],
			},
		},
		options: [
			{
				displayName: 'Disable Dot Notation',
				name: 'disableDotNotation',
				type: 'boolean',
				default: false,
				displayOptions: {
					show: {
						'/operation': ['removeDuplicateInputItems'],
					},
					hide: {
						'/compare': ['allFields'],
					},
				},
				description:
					'Whether to disallow referencing child fields using `parent.child` in the field name',
			},
			{
				displayName: 'Remove Other Fields',
				name: 'removeOtherFields',
				type: 'boolean',
				default: false,
				displayOptions: {
					show: {
						'/operation': ['removeDuplicateInputItems'],
					},
					hide: {
						'/compare': ['allFields'],
					},
				},
				description:
					'Whether to remove any fields that are not being compared. If disabled, will keep the values from the first of the duplicates.',
			},
			{
				displayName: 'Scope',
				name: 'scope',
				type: 'options',
				default: 'node',
				displayOptions: {
					show: {
						'/operation': ['clearDeduplicationHistory', 'removeItemsSeenInPreviousExecutions'],
					},
				},
				description:
					'If set to ‘workflow,’ key values will be shared across all nodes in the workflow. If set to ‘node,’ key values will be specific to this node.',
				options: [
					{
						name: 'Workflow',
						value: 'workflow',
						description: 'Deduplication info will be shared by all the nodes in the workflow',
					},
					{
						name: 'Node',
						value: 'node',
						description: 'Deduplication info will be stored only for this node',
					},
				],
			},
			{
				displayName: 'Don’t Update Key Values on Database',
				name: 'dontUpdateKeyValuesOnDatabase',
				type: 'boolean',
				default: false,
				displayOptions: {
					show: {
						'/operation': ['removeItemsSeenInPreviousExecutions'],
					},
				},
				description:
					'Whether to just remove duplicates based on the stored key values without updating the database',
			},
			{
				displayName: 'History Size',
				name: 'historySize',
				type: 'number',
				default: 1000,
				hint: 'The max number of past items to store for deduplication',
				displayOptions: {
					show: {
						'/operation': ['removeItemsSeenInPreviousExecutions'],
					},
				},
			},
		],
	},
];
