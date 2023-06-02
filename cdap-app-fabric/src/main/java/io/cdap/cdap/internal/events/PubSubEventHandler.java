package io.cdap.cdap.internal.events;

import com.google.common.util.concurrent.AbstractScheduledService;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.inject.Inject;
import io.cdap.cdap.common.TooManyRequestsException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.internal.app.services.ProgramLifecycleService;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.ProgramReference;
import io.cdap.cdap.spi.events.PubSubEventReader;
import io.cdap.cdap.spi.events.ReceivedEvent;
import io.cdap.cdap.spi.events.StartPipelineEventDetails;
import org.apache.twill.api.RunId;

import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class PubSubEventHandler extends AbstractScheduledService {


  private static final Logger logger = Logger.getLogger(PubSubEventReader.class.getName());
  private final boolean enabled;
  private final EventReaderExtensionProvider readerExtensionProvider;
  private final PubSubEventReader reader;
  private final Gson gson;

  private final ProgramLifecycleService lifecycleService;

  @Inject
  PubSubEventHandler(CConfiguration cConf,
                     EventReaderExtensionProvider readerExtensionProvider, ProgramLifecycleService lifecycleService) {
    this.enabled = true;
    //this.enabled = Feature.EVENT_PUBLISH.isEnabled(new DefaultFeatureFlagsProvider(cConf));
    this.readerExtensionProvider = readerExtensionProvider;
    reader = readerExtensionProvider.get("pub-sub-event-reader");
    reader.initialize();
    this.lifecycleService = lifecycleService;
    gson = new Gson();
  }


  @Override
  protected void runOneIteration() throws Exception {
    Optional<ReceivedEvent> pulledMessage = reader.pull();
    if (pulledMessage.isPresent()) {
      ReceivedEvent receivedMessage = pulledMessage.get();
      boolean ack = true;
      try {
        StartPipelineEventDetails eventDetails = gson.fromJson(receivedMessage.getEventDetails().getData(),
            StartPipelineEventDetails.class);
        ProgramType programType = ProgramType.valueOfCategoryName(eventDetails.getProgramType());
        ProgramReference programReference = new ProgramReference(eventDetails.getNamespaceId(),
            eventDetails.getAppId(), programType,
            eventDetails.getProgramId());
        RunId runId = lifecycleService.run(programReference, eventDetails.getUserArgs(), true);
        logger.log(Level.FINE, "Started pipeline, RunId: " + runId);
      } catch (JsonSyntaxException e) {
        logger.log(Level.SEVERE, "Cannot read JSON PubSub Message");
      } catch (TooManyRequestsException e) {
        logger.log(Level.SEVERE, "At instance capacity");
        ack = false;
      } catch (Exception e) {
        logger.log(Level.SEVERE, e.getMessage());
      }
      if (ack) {
        reader.ack(receivedMessage.getEventDetails().getAckId());
      } else {
        reader.nack(receivedMessage.getEventDetails().getAckId());
      }
    }
  }


  @Override
  protected Scheduler scheduler() {
    return Scheduler.newFixedDelaySchedule(1, 1, TimeUnit.SECONDS);
  }


}
