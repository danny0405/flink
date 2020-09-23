/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.operators.window.join;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.MergingState;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.state.internal.InternalMergingState;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.JoinedRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.operators.join.stream.AbstractStreamingJoinOperator;
import org.apache.flink.table.runtime.operators.join.stream.state.JoinInputSideSpec;
import org.apache.flink.table.runtime.operators.window.TimeWindow;
import org.apache.flink.table.runtime.operators.window.Window;
import org.apache.flink.table.runtime.operators.window.assigners.InternalTimeWindowAssigner;
import org.apache.flink.table.runtime.operators.window.assigners.MergingWindowAssigner;
import org.apache.flink.table.runtime.operators.window.assigners.PanedWindowAssigner;
import org.apache.flink.table.runtime.operators.window.assigners.SessionWindowAssigner;
import org.apache.flink.table.runtime.operators.window.assigners.SlidingWindowAssigner;
import org.apache.flink.table.runtime.operators.window.assigners.TumblingWindowAssigner;
import org.apache.flink.table.runtime.operators.window.assigners.WindowAssigner;
import org.apache.flink.table.runtime.operators.window.internal.InternalWindowProcessFunction;
import org.apache.flink.table.runtime.operators.window.internal.JoinGeneralWindowProcessFunction;
import org.apache.flink.table.runtime.operators.window.internal.JoinMergingWindowProcessFunction;
import org.apache.flink.table.runtime.operators.window.internal.JoinPanedWindowProcessFunction;
import org.apache.flink.table.runtime.operators.window.internal.WindowJoinProcessFunction;
import org.apache.flink.table.runtime.operators.window.join.state.WindowJoinRecordStateView;
import org.apache.flink.table.runtime.operators.window.join.state.WindowJoinRecordStateViews;
import org.apache.flink.table.runtime.operators.window.join.state.WindowedStateView;
import org.apache.flink.table.runtime.operators.window.triggers.EventTimeTriggers;
import org.apache.flink.table.runtime.operators.window.triggers.ProcessingTimeTriggers;
import org.apache.flink.table.runtime.operators.window.triggers.Trigger;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Preconditions;

import java.time.Duration;
import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import static java.util.Objects.requireNonNull;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An operator that implements the logic for join streams windowing
 * based on a {@link WindowAssigner} and {@link Trigger}.
 *
 * <p>When an element arrives it gets assigned a key using a {@link KeySelector} and it gets
 * assigned to zero or more windows using a {@link WindowAssigner}. Based on this, the element
 * is put into panes. A pane is the bucket of elements that have the same key and same
 * {@code Window}. An element can be in multiple panes if it was assigned to multiple windows by
 * the {@code WindowAssigner}.
 *
 * <p>Each input side of the join window the streams individually. They expect to
 * have the same windowing strategy, e.g. the window type and window parameters.
 * When the LHS element triggers the window firing
 * (e.g. window1 max timestamp >= current watermark),
 * that means the same window on the RHS also meet the condition and should be fired.
 * We then get the triggered window records from the views of both sides,
 * join them with the given condition and emit. Note that the inputs are buffered in the state
 * and only emitted when a window fires. The buffered data is cleaned atomically when the window
 * expires.
 *
 * <pre>
 *                    input1                 input2
 *                      |                      |
 *                |  window1 |  &lt;= join =&gt; | window2 |
 * </pre>
 *
 * <p>A window join triggers when:
 * <ul>
 *     <li>Element from LHS or RHS triggers and fires the window</li>
 *     <li>Registered event-time timers trigger</li>
 *     <li>Registered processing-time timers trigger</li>
 * </ul>
 *
 * <p>Each pane gets its own instance of the provided {@code Trigger}. This trigger determines when
 * the contents of the pane should be processed to emit results. When a trigger fires,
 * the input views {@link WindowJoinRecordStateView} produces the join records
 * under the current trigger namespace to join and emit for the pane to which the {@code Trigger}
 * belongs.
 *
 * <p>The join output type should be: (left_type, right_type, window_start, window_end).
 *
 * @param <K> The type of key returned by the {@code KeySelector}.
 * @param <W> The type of {@code Window} that the {@code WindowAssigner} assigns.
 */
public class WindowJoinOperator<K, W extends Window>
		extends AbstractStreamingJoinOperator
		implements Triggerable<K, W> {

	private static final long serialVersionUID = 1L;

	private static final String LATE_ELEMENTS_DROPPED_METRIC_NAME = "numLateRecordsDropped";
	private static final String LATE_ELEMENTS_DROPPED_RATE_METRIC_NAME = "lateRecordsDroppedRate";
	private static final String WATERMARK_LATENCY_METRIC_NAME = "watermarkLatency";

	// -------------------------------------------------------------------------
	//  Window Configuration
	// -------------------------------------------------------------------------

	private final WindowAssigner<W> windowAssigner;

	private final Trigger<W> trigger;

	/** For serializing the window in checkpoints. */
	private final TypeSerializer<W> windowSerializer;

	/**
	 * The allowed lateness for elements. This is used for:
	 * <ul>
	 * <li>Deciding if an element should be dropped from a window due to lateness.
	 * <li>Clearing the state of a window if the system time passes the
	 * {@code window.maxTimestamp + allowedLateness} landmark.
	 * </ul>
	 */
	private final long allowedLateness;

	/** Process function for first input. **/
	protected transient WindowJoinProcessFunction<K, W> windowFunction1;

	/** Process function for second input. **/
	protected transient WindowJoinProcessFunction<K, W> windowFunction2;

	private transient InternalTimerService<W> internalTimerService;

	private transient TriggerContext triggerContext;

	private final int leftRowtimeIndex;

	private final int rightRowtimeIndex;

	private final WindowAttribute leftWindowAttr;

	private final WindowAttribute rightWindowAttr;

	// -------------------------------------------------------------------------
	//  Join Configuration
	// -------------------------------------------------------------------------

	// whether left hand side can generate nulls
	private final boolean generateNullsOnLeft;
	// whether right hand side can generate nulls
	private final boolean generateNullsOnRight;

	// used to output left data row and window attributes
	private transient WindowAttrCollectors.WindowAttrCollector leftCollector;
	// used to output right data row and window attributes
	private transient WindowAttrCollectors.WindowAttrCollector rightCollector;
	private transient RowData leftNullRow;
	private transient RowData rightNullRow;
	// output row: data row + window attributes
	private transient JoinedRowData outputRow;

	// left join state
	private transient WindowJoinRecordStateView<W> joinInputView1;
	// right join state
	private transient WindowJoinRecordStateView<W> joinInputView2;

	// ------------------------------------------------------------------------
	// Metrics
	// ------------------------------------------------------------------------

	private transient Counter numLateRecordsDropped;
	private transient Meter lateRecordsDroppedRate;
	private transient Gauge<Long> watermarkLatency;

	private WindowJoinOperator(
			WindowAssigner<W> windowAssigner,
			Trigger<W> trigger,
			TypeSerializer<W> windowSerializer,
			long allowedLateness,
			InternalTypeInfo<RowData> leftType,
			InternalTypeInfo<RowData> rightType,
			GeneratedJoinCondition generatedJoinCondition,
			JoinInputSideSpec leftInputSideSpec,
			JoinInputSideSpec rightInputSideSpec,
			boolean generateNullsOnLeft,
			boolean generateNullsOnRight,
			int leftRowtimeIndex,
			int rightRowtimeIndex,
			WindowAttribute leftWindowAttr,
			WindowAttribute rightWindowAttr,
			boolean[] filterNullKeys) {
		super(leftType, rightType, generatedJoinCondition, leftInputSideSpec,
				rightInputSideSpec, filterNullKeys, -1);
		this.windowAssigner = checkNotNull(windowAssigner);
		this.trigger = checkNotNull(trigger);
		this.windowSerializer = checkNotNull(windowSerializer);
		checkArgument(allowedLateness >= 0);
		this.allowedLateness = allowedLateness;
		this.generateNullsOnLeft = generateNullsOnLeft;
		this.generateNullsOnRight = generateNullsOnRight;
		checkArgument(!windowAssigner.isEventTime()
				|| leftRowtimeIndex >= 0 && rightRowtimeIndex >= 0);
		this.leftRowtimeIndex = leftRowtimeIndex;
		this.rightRowtimeIndex = rightRowtimeIndex;
		this.leftWindowAttr = leftWindowAttr;
		this.rightWindowAttr = rightWindowAttr;
	}

	@Override
	public void open() throws Exception {
		super.open();

		internalTimerService = getInternalTimerService("window-join-timers",
				windowSerializer, this);

		triggerContext = new TriggerContext();
		triggerContext.open();

		this.leftCollector = WindowAttrCollectors.getWindowAttrCollector(leftWindowAttr);
		this.rightCollector = WindowAttrCollectors.getWindowAttrCollector(rightWindowAttr);
		this.outputRow = new JoinedRowData();
		this.leftNullRow = new GenericRowData(leftType.toRowType().getFieldCount());
		this.rightNullRow = new GenericRowData(rightType.toRowType().getFieldCount());

		ViewContext viewContext = new ViewContext();
		// initialize states
		this.joinInputView1 = WindowJoinRecordStateViews.create(
				viewContext,
				windowSerializer,
				"left-records",
				leftInputSideSpec,
				leftType);

		this.joinInputView2 = WindowJoinRecordStateViews.create(
				viewContext,
				windowSerializer,
				"right-records",
				rightInputSideSpec,
				rightType);

		WindowContext windowContext1 = new WindowContext(this.joinInputView1);
		WindowContext windowContext2 = new WindowContext(this.joinInputView2);
		if (windowAssigner instanceof MergingWindowAssigner) {
			this.windowFunction1 = new JoinMergingWindowProcessFunction<>(
					(MergingWindowAssigner<W>) windowAssigner,
					"window-to-state-1",
					joinInputView1,
					windowSerializer,
					allowedLateness);
			this.windowFunction2 = new JoinMergingWindowProcessFunction<>(
					(MergingWindowAssigner<W>) windowAssigner,
					"window-to-state-2",
					joinInputView2,
					windowSerializer,
					allowedLateness);
		} else if (windowAssigner instanceof PanedWindowAssigner) {
			this.windowFunction1 = new JoinPanedWindowProcessFunction<>(
					(PanedWindowAssigner<W>) windowAssigner,
					joinInputView1,
					allowedLateness);
			this.windowFunction2 = new JoinPanedWindowProcessFunction<>(
					(PanedWindowAssigner<W>) windowAssigner,
					joinInputView2,
					allowedLateness);
		} else {
			this.windowFunction1 = new JoinGeneralWindowProcessFunction<>(
					windowAssigner,
					joinInputView1,
					allowedLateness);
			this.windowFunction2 = new JoinGeneralWindowProcessFunction<>(
					windowAssigner,
					joinInputView2,
					allowedLateness);
		}
		this.windowFunction1.open(windowContext1);
		this.windowFunction2.open(windowContext2);

		// metrics
		numLateRecordsDropped = metrics.counter(LATE_ELEMENTS_DROPPED_METRIC_NAME);
		this.lateRecordsDroppedRate = metrics.meter(
				LATE_ELEMENTS_DROPPED_RATE_METRIC_NAME,
				new MeterView(numLateRecordsDropped));
		this.watermarkLatency = metrics.gauge(WATERMARK_LATENCY_METRIC_NAME, () -> {
			long watermark = internalTimerService.currentWatermark();
			if (watermark < 0) {
				return 0L;
			} else {
				return internalTimerService.currentProcessingTime() - watermark;
			}
		});
	}

	@Override
	public void close() throws Exception {
		super.close();
		collector = null;
		triggerContext = null;
	}

	@Override
	public void dispose() throws Exception {
		super.dispose();
		collector = null;
		triggerContext = null;
	}

	@Override
	public void processElement1(StreamRecord<RowData> element) throws Exception {
		processElement(element, true);
	}

	@Override
	public void processElement2(StreamRecord<RowData> element) throws Exception {
		processElement(element, false);
	}

	private void processElement (StreamRecord<RowData> element, boolean isFirstInput)
			throws Exception {
		final int rowtimeIndex = isFirstInput ? leftRowtimeIndex : rightRowtimeIndex;
		final InternalWindowProcessFunction<K, W> windowFunction = isFirstInput ? windowFunction1 : windowFunction2;
		final WindowJoinRecordStateView<W> joinInputView = isFirstInput ? joinInputView1 : joinInputView2;
		RowData inputRow = element.getValue();

		final long timestamp = windowAssigner.isEventTime()
				? inputRow.getLong(rowtimeIndex)
				: internalTimerService.currentProcessingTime();

		// the windows which the input row should be placed into
		Collection<W> stateWindows = windowFunction.assignStateNamespace(inputRow, timestamp);
		boolean isElementDropped = true;
		for (W window : stateWindows) {
			isElementDropped = false;
			joinInputView.setCurrentNamespace(window);
			if (inputRow.getRowKind() == RowKind.UPDATE_BEFORE) {
				// Ignore update before message.
				continue;
			}
			if (inputRow.getRowKind() == RowKind.DELETE) {
				// Erase RowKind for state updating
				inputRow.setRowKind(RowKind.INSERT);
				joinInputView.retractRecord(inputRow);
			} else {
				joinInputView.addRecord(inputRow);
			}
		}

		// the actual window which the input row is belongs to
		Collection<W> actualWindows = windowFunction.assignActualWindows(inputRow, timestamp);
		for (W window : actualWindows) {
			isElementDropped = false;
			triggerContext.window = window;
			boolean triggerResult = triggerContext.onElement(inputRow, timestamp);
			if (triggerResult) {
				joinWindowInputsAndEmit(window);
			}
			// register a clean up timer for the window
			registerCleanupTimer(window);
		}

		if (isElementDropped) {
			// markEvent will increase numLateRecordsDropped
			lateRecordsDroppedRate.markEvent();
		}
	}

	@Override
	public void onEventTime(InternalTimer<K, W> timer) throws Exception {
		setCurrentKey(timer.getKey());

		triggerContext.window = timer.getNamespace();
		if (triggerContext.onEventTime(timer.getTimestamp())) {
			// fire
			joinWindowInputsAndEmit(triggerContext.window);
		}

		if (windowAssigner.isEventTime()) {
			windowFunction1.cleanWindowIfNeeded(triggerContext.window, timer.getTimestamp());
			windowFunction2.cleanWindowIfNeeded(triggerContext.window, timer.getTimestamp());
		}
	}

	@Override
	public void onProcessingTime(InternalTimer<K, W> timer) throws Exception {
		setCurrentKey(timer.getKey());

		triggerContext.window = timer.getNamespace();
		if (triggerContext.onProcessingTime(timer.getTimestamp())) {
			// fire
			joinWindowInputsAndEmit(triggerContext.window);
		}

		if (!windowAssigner.isEventTime()) {
			windowFunction1.cleanWindowIfNeeded(triggerContext.window, timer.getTimestamp());
			windowFunction2.cleanWindowIfNeeded(triggerContext.window, timer.getTimestamp());
		}
	}

	// -------------------------------------------------------------------------
	//  Inner Class
	// -------------------------------------------------------------------------

	/**
	 * Context of window.
	 */
	private class WindowContext implements InternalWindowProcessFunction.Context<K, W> {
		private WindowJoinRecordStateView<W> view;

		WindowContext(WindowJoinRecordStateView<W> view) {
			this.view = view;
		}

		@Override
		public <S extends State> S getPartitionedState(
				StateDescriptor<S, ?> stateDescriptor) throws Exception {
			requireNonNull(stateDescriptor, "The state properties must not be null");
			return WindowJoinOperator.this.getPartitionedState(stateDescriptor);
		}

		@Override
		public K currentKey() {
			return WindowJoinOperator.this.currentKey();
		}

		@Override
		public long currentProcessingTime() {
			return internalTimerService.currentProcessingTime();
		}

		@Override
		public long currentWatermark() {
			return internalTimerService.currentWatermark();
		}

		@Override
		public void clearWindowState(W window) {
			this.view.setCurrentNamespace(window);
			this.view.clear();
		}

		@Override
		public void clearTrigger(W window) throws Exception {
			triggerContext.window = window;
			triggerContext.clear();
		}

		@Override
		public void deleteCleanupTimer(W window) {
			long cleanupTime = cleanupTime(window);
			if (cleanupTime == Long.MAX_VALUE) {
				// no need to clean up because we didn't set one
				return;
			}
			if (windowAssigner.isEventTime()) {
				triggerContext.deleteEventTimeTimer(cleanupTime);
			} else {
				triggerContext.deleteProcessingTimeTimer(cleanupTime);
			}
		}

		@Override
		public void onMerge(W newWindow, Collection<W> mergedWindows) throws Exception {
			triggerContext.window = newWindow;
			triggerContext.mergedWindows = mergedWindows;
			triggerContext.onMerge();
		}
	}

	/**
	 * {@code TriggerContext} is a utility for handling {@code Trigger} invocations. It can be
	 * reused by setting the {@code key} and {@code window} fields.
	 * Non-internal state must be kept in the {@code TriggerContext}.
	 */
	private class TriggerContext implements Trigger.OnMergeContext {

		private W window;
		private Collection<W> mergedWindows;

		public void open() throws Exception {
			trigger.open(this);
		}

		boolean onElement(RowData row, long timestamp) throws Exception {
			return trigger.onElement(row, timestamp, window);
		}

		boolean onProcessingTime(long time) throws Exception {
			return trigger.onProcessingTime(time, window);
		}

		boolean onEventTime(long time) throws Exception {
			return trigger.onEventTime(time, window);
		}

		void onMerge() throws Exception {
			trigger.onMerge(window, this);
		}

		@Override
		public long getCurrentProcessingTime() {
			return internalTimerService.currentProcessingTime();
		}

		@Override
		public long getCurrentWatermark() {
			return internalTimerService.currentWatermark();
		}

		@Override
		public MetricGroup getMetricGroup() {
			return WindowJoinOperator.this.getMetricGroup();
		}

		@Override
		public void registerProcessingTimeTimer(long time) {
			internalTimerService.registerProcessingTimeTimer(window, time);
		}

		@Override
		public void registerEventTimeTimer(long time) {
			internalTimerService.registerEventTimeTimer(window, time);
		}

		@Override
		public void deleteProcessingTimeTimer(long time) {
			internalTimerService.deleteProcessingTimeTimer(window, time);
		}

		@Override
		public void deleteEventTimeTimer(long time) {
			internalTimerService.deleteEventTimeTimer(window, time);
		}

		public void clear() throws Exception {
			trigger.clear(window);
		}

		@Override
		public <S extends State> S getPartitionedState(StateDescriptor<S, ?> stateDescriptor) {
			try {
				return WindowJoinOperator.this.getPartitionedState(window, windowSerializer, stateDescriptor);
			} catch (Exception e) {
				throw new RuntimeException("Could not retrieve state", e);
			}
		}

		@Override
		@SuppressWarnings("unchecked")
		public <S extends MergingState<?, ?>> void mergePartitionedState(
				StateDescriptor<S, ?> stateDescriptor) {
			if (mergedWindows != null && mergedWindows.size() > 0) {
				try {
					State state =
							WindowJoinOperator.this.getOrCreateKeyedState(
									windowSerializer,
									stateDescriptor);
					if (state instanceof InternalMergingState) {
						((InternalMergingState<K, W, ?, ?, ?>) state).mergeNamespaces(window, mergedWindows);
					} else {
						throw new IllegalArgumentException(
								"The given state descriptor does not refer to a mergeable state (MergingState)");
					}
				}
				catch (Exception e) {
					throw new RuntimeException("Error while merging state.", e);
				}
			}
		}
	}

	/** Context of {@code WindowedStateView}. */
	private class ViewContext implements WindowedStateView.Context {
		@Override
		public <S extends State, N extends Window> S getOrCreateKeyedState(
				TypeSerializer<N> windowSerializer,
				StateDescriptor<S, ?> stateDescriptor) throws Exception {
			return WindowJoinOperator.this.getOrCreateKeyedState(windowSerializer, stateDescriptor);
		}
	}

	/** Builder for {@link WindowJoinOperator}. */
	public static class Builder {
		private WindowAssigner<?> windowAssigner;
		private Trigger<?> trigger;
		private GeneratedJoinCondition joinCondition;
		private InternalTypeInfo<RowData> type1;
		private InternalTypeInfo<RowData> type2;
		private JoinInputSideSpec inputSideSpec1;
		private JoinInputSideSpec inputSideSpec2;
		private Boolean generateNullsOnLeft;
		private Boolean generateNullsOnRight;
		private long allowedLateness = 0L;
		private int rowtimeIndex1 = -1;
		private int rowtimeIndex2 = -1;
		private WindowAttribute leftAttr = WindowAttribute.NONE;
		private WindowAttribute rightAttr = WindowAttribute.START_END;
		private boolean[] filterNullKeys;

		public Builder inputType(
				InternalTypeInfo<RowData> type1,
				InternalTypeInfo<RowData> type2) {
			this.type1 = Objects.requireNonNull(type1);
			this.type2 = Objects.requireNonNull(type2);
			return this;
		}

		public Builder joinType(FlinkJoinType joinType) {
			checkArgument(joinType != FlinkJoinType.ANTI && joinType != FlinkJoinType.SEMI,
					"Unsupported join type: " + joinType);
			this.generateNullsOnLeft = joinType.isRightOuter();
			this.generateNullsOnRight = joinType.isLeftOuter();
			return this;
		}

		public Builder joinCondition(GeneratedJoinCondition joinCondition) {
			this.joinCondition = Objects.requireNonNull(joinCondition);
			return this;
		}

		public Builder joinInputSpec(
				JoinInputSideSpec inputSideSpec1,
				JoinInputSideSpec inputSideSpec2) {
			this.inputSideSpec1 = Objects.requireNonNull(inputSideSpec1);
			this.inputSideSpec2 = Objects.requireNonNull(inputSideSpec2);
			return this;
		}

		/** Array of booleans to describe whether each equal join key needs to filter out nulls,
		 * thus, we can distinguish between EQUALS and IS NOT DISTINCT FROM. */
		public Builder filterNullKeys(boolean... filterNullKeys) {
			this.filterNullKeys = Objects.requireNonNull(filterNullKeys);
			return this;
		}

		public Builder assigner(WindowAssigner<?> assigner) {
			this.windowAssigner = Objects.requireNonNull(assigner);
			return this;
		}

		public Builder trigger(Trigger<?> trigger) {
			this.trigger = Objects.requireNonNull(trigger);
			return this;
		}

		public Builder tumble(Duration size) {
			checkArgument(
					windowAssigner == null,
					"Tumbling window properties already been set");
			this.windowAssigner = TumblingWindowAssigner.of(size);
			return this;
		}

		public Builder sliding(Duration size, Duration slide) {
			checkArgument(
					windowAssigner == null,
					"Sliding window properties already been set");
			this.windowAssigner = SlidingWindowAssigner.of(size, slide);
			return this;
		}

		public Builder session(Duration sessionGap) {
			checkArgument(
					windowAssigner == null,
					"Session window properties already been set");
			this.windowAssigner = SessionWindowAssigner.withGap(sessionGap);
			return this;
		}

		public Builder eventTime(int leftIndex, int rightIndex) {
			checkArgument(windowAssigner != null,
					"Use Builder.tumble, Builder.sliding or Builder.session or Builder.assigner to "
							+ "set up the window assigning strategies first");
			if (windowAssigner instanceof InternalTimeWindowAssigner) {
				InternalTimeWindowAssigner timeWindowAssigner = (InternalTimeWindowAssigner) windowAssigner;
				this.windowAssigner = (WindowAssigner<?>) timeWindowAssigner.withEventTime();
			}
			this.rowtimeIndex1 = leftIndex;
			this.rowtimeIndex2 = rightIndex;
			if (trigger == null) {
				this.trigger = EventTimeTriggers.afterEndOfWindow();
			}
			return this;
		}

		public Builder processingTime() {
			checkArgument(windowAssigner != null,
					"Use Builder.tumble, Builder.sliding or Builder.session or Builder.assigner to "
							+ "set up the window assigning strategies first");
			if (windowAssigner instanceof InternalTimeWindowAssigner) {
				InternalTimeWindowAssigner timeWindowAssigner = (InternalTimeWindowAssigner) windowAssigner;
				this.windowAssigner = (WindowAssigner<?>) timeWindowAssigner.withProcessingTime();
			}
			if (trigger == null) {
				this.trigger = ProcessingTimeTriggers.afterEndOfWindow();
			}
			return this;
		}

		public Builder allowedLateness(Duration allowedLateness) {
			checkArgument(!allowedLateness.isNegative());
			if (allowedLateness.toMillis() > 0) {
				this.allowedLateness = allowedLateness.toMillis();
			}
			return this;
		}

		public Builder windowAttribute(WindowAttribute leftAttr, WindowAttribute rightAttr) {
			this.leftAttr = Objects.requireNonNull(leftAttr);
			this.rightAttr = Objects.requireNonNull(rightAttr);
			return this;
		}

		@SuppressWarnings("unchecked")
		public <K, W extends Window> WindowJoinOperator<K, W> build() {
			Preconditions.checkState(this.type1 != null && this.type2 != null,
					"Use Builder.inputType to set up the join input data types");
			Preconditions.checkState(this.generateNullsOnLeft != null,
					"Use Builder.joinType to set up the join type");
			Preconditions.checkState(this.joinCondition != null,
					"Use Builder.joinCondition to set up the join condition");
			Preconditions.checkState(this.inputSideSpec1 != null && this.inputSideSpec2 != null,
					"Use Builder.joinInputSpec to set up the join input specifications");
			Preconditions.checkState(this.filterNullKeys != null,
					"Use Builder.filterNullKeys to set up the which join keys need to filter nulls");
			Preconditions.checkState(this.windowAssigner != null,
					"Use Builder.tumble, Builder.sliding or Builder.session or Builder.assigner to "
							+ "set up the window assigning strategies");
			Preconditions.checkState(this.trigger != null,
					"Use Builder.eventTime or Builder.processingTime or Builder.trigger "
							+ "to set up the window triggering strategy");
			return new WindowJoinOperator<>(
					(WindowAssigner<W>) this.windowAssigner,
					(Trigger<W>) this.trigger,
					(TypeSerializer<W>) this.windowAssigner.getWindowSerializer(new ExecutionConfig()),
					this.allowedLateness,
					this.type1,
					this.type2,
					this.joinCondition,
					this.inputSideSpec1,
					this.inputSideSpec2,
					this.generateNullsOnLeft,
					this.generateNullsOnRight,
					this.rowtimeIndex1,
					this.rowtimeIndex2,
					this.leftAttr,
					this.rightAttr,
					this.filterNullKeys);
		}
	}

	// -------------------------------------------------------------------------
	//  Utilities
	// -------------------------------------------------------------------------

	/** Returns the builder of {@link WindowJoinOperator}. */
	public static Builder builder() {
		return new Builder();
	}

	/**
	 * Emits the window result of the given window.
	 */
	private void joinWindowInputsAndEmit(W window) throws Exception {
		Iterable<RowData> leftInputs = this.windowFunction1.prepareInputsToJoin(window);
		Iterable<RowData> rightInputs = this.windowFunction2.prepareInputsToJoin(window);
		if (generateNullsOnLeft) {
			joinLeftIsOuter(window, leftInputs, rightInputs, generateNullsOnRight);
		} else {
			joinLeftNonOuter(window, leftInputs, rightInputs, generateNullsOnRight);
		}
	}

	private void joinLeftNonOuter(
			W window,
			Iterable<RowData> leftInputs,
			Iterable<RowData> rightInputs,
			boolean rightGeneratesNulls) {
		for (RowData left : leftInputs) {
			boolean leftMatched = false;
			for (RowData right : rightInputs) {
				boolean matches = joinCondition.apply(left, right);
				if (matches) {
					leftMatched = true;
					output(left, right, window);
				}
			}
			if (!leftMatched && rightGeneratesNulls) {
				outputNullPadding(left, true, window);
			}
		}
	}

	private void joinLeftIsOuter(
			W window,
			Iterable<RowData> leftInputs,
			Iterable<RowData> rightInputs,
			boolean rightGeneratesNulls) {
		Set<Integer> rightMatchedIndices = new HashSet<>();
		for (RowData left : leftInputs) {
			boolean leftMatched = false;
			int idx = 0;
			for (RowData right : rightInputs) {
				boolean matches = joinCondition.apply(left, right);
				if (matches) {
					leftMatched = true;
					output(left, right, window);
					rightMatchedIndices.add(idx);
				}
				idx++;
			}
			if (!leftMatched && rightGeneratesNulls) {
				outputNullPadding(left, true, window);
			}
		}
		int idx = 0;
		for (RowData right : rightInputs) {
			if (!rightMatchedIndices.contains(idx)) {
				outputNullPadding(right, false, window);
			}
			idx++;
		}
	}

	/**
	 * Registers a timer to cleanup the content of the window.
	 *
	 * @param window the window whose state to discard
	 */
	private void registerCleanupTimer(W window) {
		long cleanupTime = cleanupTime(window);
		if (cleanupTime == Long.MAX_VALUE) {
			// don't set a GC timer for "end of time"
			return;
		}

		if (windowAssigner.isEventTime()) {
			triggerContext.registerEventTimeTimer(cleanupTime);
		} else {
			triggerContext.registerProcessingTimeTimer(cleanupTime);
		}
	}

	/**
	 * Returns the cleanup time for a window, which is
	 * {@code window.maxTimestamp + allowedLateness}. In
	 * case this leads to a value greater than {@link Long#MAX_VALUE}
	 * then a cleanup time of {@link Long#MAX_VALUE} is
	 * returned.
	 *
	 * @param window the window whose cleanup time we are computing.
	 */
	private long cleanupTime(W window) {
		if (windowAssigner.isEventTime()) {
			long cleanupTime = Math.max(0, window.maxTimestamp() + allowedLateness);
			return cleanupTime >= window.maxTimestamp() ? cleanupTime : Long.MAX_VALUE;
		} else {
			return Math.max(0, window.maxTimestamp());
		}
	}

	@SuppressWarnings("unchecked")
	private K currentKey() {
		return (K) getCurrentKey();
	}

	private void output(RowData inputRow, RowData otherRow, W window) {
		RowData left = leftCollector.collect(inputRow, (TimeWindow) window);
		RowData right = rightCollector.collect(otherRow, (TimeWindow) window);

		outputRow.replace(left, right);
		collector.collect(outputRow);
	}

	private void outputNullPadding(RowData row, boolean isLeft, W window) {
		RowData leftData;
		RowData rightData;
		if (isLeft) {
			leftData = leftCollector.collect(row, (TimeWindow) window);
			rightData = rightCollector.collect(rightNullRow, (TimeWindow) window);
		} else {
			leftData = leftCollector.collect(leftNullRow, (TimeWindow) window);
			rightData = rightCollector.collect(row, (TimeWindow) window);
		}
		outputRow.replace(leftData, rightData);
		collector.collect(outputRow);
	}

	// ------------------------------------------------------------------------------
	// Visible For Testing
	// ------------------------------------------------------------------------------

	@VisibleForTesting
	protected Counter getNumLateRecordsDropped() {
		return numLateRecordsDropped;
	}

	@VisibleForTesting
	protected Gauge<Long> getWatermarkLatency() {
		return watermarkLatency;
	}
}
