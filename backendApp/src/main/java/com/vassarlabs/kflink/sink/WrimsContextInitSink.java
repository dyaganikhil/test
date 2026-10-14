package com.vassarlabs.kflink.sink;

import com.vassarlabs.cass.eventmapper.utils.EventConstants;
import com.vassarlabs.common.dsp.err.DSPException;
import com.vassarlabs.common.init.err.AppInitializationException;
import com.vassarlabs.common.init.service.StormInitServiceImpl;
import com.vassarlabs.common.init.service.api.IApplicationInitService;
import com.vassarlabs.config.BusinessComponentScan;
import com.vassarlabs.config.PlatformComponentScan;
import com.vassarlabs.config.spring.AppContext;
import com.vassarlabs.kflink.sink.impl.AVLFlinkSink;
import com.vassarlabs.kstorm.msg.api.IKafkaMessageObject;
import com.vassarlabs.location.service.api.ILocationHierarchyService;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

/**
 * Initializes the Spring AnnotationConfigApplicationContext on Flink TaskManagers.
 * Also runs StormInit (Quartz) and pre-loads the location cache.
 *
 * Sets AVLFlinkSink.contextReady = true after full initialization so that all
 * other sinks (which block in open() waiting for this flag) are guaranteed to
 * have the context available before they start processing any records.
 *
 * Must be registered in CreatePipeline before all other pipelines.
 */
public class WrimsContextInitSink<Z> extends AVLFlinkSink<Z> {

    private static final long serialVersionUID = 1L;

    private static volatile boolean contextInitialized = false;

    // Guards the one-time context init below. Was AppContext.LOCK_OBJECT, which wrims-core-platform removed
    // together with AppContext's fallback-context creation (the only other code that used it).
    private static final Object INIT_LOCK = new Object();

    @Override
    public void setRuntimeContext(RuntimeContext t) {
        if (!contextInitialized) {
            synchronized (INIT_LOCK) {
                if (!contextInitialized) {
                    System.out.println("WrimsContextInitSink: initializing Spring context on thread: "
                            + Thread.currentThread().getName());

                    AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext(
                            PlatformComponentScan.class,
                            BusinessComponentScan.class
                    );
                    AppContext.setApplicationContext(ctx);

                    // StormInit: starts Quartz scheduler
                    try {
                        IApplicationInitService stormInit = ctx.getBean(StormInitServiceImpl.class);
                        stormInit.initialize();
                    } catch (AppInitializationException e) {
                        throw new RuntimeException("WrimsContextInitSink: StormInit failed", e);
                    }

                    // Pre-load location cache
                    try {
                        ILocationHierarchyService locService = ctx.getBean(ILocationHierarchyService.class);
                        System.out.println("WrimsContextInitSink: cache initialized: "
                                + locService.getLocNameForLocUUID("d6b37905-d2d3-4275-9317-d9b6f47cd783", ""));
                    } catch (DSPException ignored) {
                    }

                    contextInitialized = true;

                    // Signal all other sinks that context is ready — they are blocked in open() waiting for this
                    AVLFlinkSink.contextReady = true;
                    System.out.println("WrimsContextInitSink: context ready, other sinks may proceed");
                }
            }
        }
        super.setRuntimeContext(t);
    }

    /**
     * Override to skip the wait-for-contextReady block in AVLFlinkSink.open().
     * This sink IS the initializer, so it must not wait on itself.
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        // intentionally skip super.open() wait — we are the one setting contextReady
    }

    /**
     * Fully override invoke() so the parent's deserialization/de-dup logic never runs.
     * This sink exists only for context initialization — it should never process records.
     */
    @Override
    public void invoke(Z record) {
        // intentional no-op
    }

    @Override
    public String getTopicName() {
        return EventConstants.WRIMS_CONTEXT_INIT_EVENT;
    }

    @Override
    public String execute(IKafkaMessageObject kafkaMessageObject) {
        // intentional no-op
        return null;
    }
}
