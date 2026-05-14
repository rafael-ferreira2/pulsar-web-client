package com.ppb.gamestate.api.outbound.util;

import com.google.protobuf.Timestamp;
import com.ppb.gamestate.api.outbound.GameStateOutbound.Api;
import com.ppb.gamestate.api.outbound.common.GameStateApiOutboundCommon;
import com.ppb.gamestate.api.outbound.fixture.GameStateApiOutboundFixture;
import com.ppb.gamestate.api.outbound.incident.GameStateApiOutboundIncident;
import com.ppb.gamestate.api.outbound.meta.GameStateApiOutboundMeta;
import com.ppb.gamestate.api.outbound.participant.GameStateApiOutboundParticipant;
import com.ppb.gamestate.api.outbound.sport.soccer.GameStateApiOutboundSportSoccer;
import com.ppb.gamestate.api.outbound.stat.GameStateApiOutboundStats;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.util.Arrays;
import java.util.UUID;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class OutboundApiFixture {

    public static final Api.Builder BASE_STATE = Api.newBuilder();

    static {
        final String SPORTS_BOOK_ID = "834232";
        final String HOME_EXTERNAL_ID = "1000";
        final String AWAY_EXTERNAL_ID = "2000";
        final String CORRELATION_ID = "test correlation id";
        final String MSG1_ID = "test message id";
        final String HOME_PLAYER_ID = "test player id";
        final long testTime = 100000000000L;
        final String incidentID = "test incident id";

        BASE_STATE
                .setMeta(GameStateApiOutboundMeta.Meta.newBuilder()
                        .setVersion("1")
                        .setHighWaterMark(3)
                        .setSport(GameStateApiOutboundMeta.Meta.Sport.SOCCER)
                        .setGameStateId(MSG1_ID)
                        .setIngressCorrelationId(CORRELATION_ID)
                        .setGameStateId(UUID.randomUUID().toString())
                        .setGameStatePreviousId(UUID.randomUUID().toString())
                        .setInstructionType(GameStateApiOutboundMeta.Meta.InstructionType.INCIDENT_UPDATE)
                        .addFeedStatus(buildFeedStatus())
                        .setCollectorInMs(System.currentTimeMillis() - 3000)
                        .setCollectorOutMs(System.currentTimeMillis() - 2000)
                        .setAggregatorInMs(System.currentTimeMillis() - 1000)
                        .setAggregatorOutMs(System.currentTimeMillis())
                        .putHeaders("receivedIn",  String.valueOf(System.currentTimeMillis() / 1000))
                        .putHeaders("publishedOut", String.valueOf(System.currentTimeMillis() / 1000)))

                .setFixture(GameStateApiOutboundFixture.Fixture.newBuilder()
                        .setSportsBookId(SPORTS_BOOK_ID)
                        .setExpectedStartTime(Timestamp.newBuilder().setSeconds(testTime))
                        .setExpectedEndTime(Timestamp.newBuilder().setSeconds(testTime))
                        .setActualStartTime(Timestamp.newBuilder().setSeconds(testTime))
                        .setStatus(GameStateApiOutboundFixture.Fixture.FixtureStatus.IN_RUNNING)
                        .setGroup(GameStateApiOutboundFixture.Fixture.FixtureGroup.newBuilder()
                                .setId("124")
                                .setDescription("Champions League")
                                .addExternalRef(GameStateApiOutboundCommon.ExternalRef.newBuilder()
                                        .setProvider(GameStateApiOutboundCommon.Provider.OPTA)
                                        .setExternalId("1000")))
                        .addExternalRef(GameStateApiOutboundCommon.ExternalRef.newBuilder()
                                .setProvider(GameStateApiOutboundCommon.Provider.SPORTEX)
                                .setExternalId("1100"))
                        .setAttributes(GameStateApiOutboundFixture.Fixture.FixtureAttributes.newBuilder()
                            .setSoccerAttributes(GameStateApiOutboundSportSoccer.SoccerAttributes.newBuilder()
                                    .setHomeParticipantRef(HOME_EXTERNAL_ID)
                                    .setAwayParticipantRef(AWAY_EXTERNAL_ID)
                                    .setFirstLegScore(GameStateApiOutboundSportSoccer.SoccerAttributes.SoccerAttributesFirstLegScore.newBuilder()
                                            .setHomeParticipant(1)
                                            .setAwayParticipant(2))
                                    .setMatchType(GameStateApiOutboundSportSoccer.SoccerAttributes.SoccerAttributesMatchType.SECOND_LEG_KNOCKOUT_WITH_AWAY_GOALS)
                                    .setPenaltyShootoutFormat("ABAB"))))
                .setStats(GameStateApiOutboundStats.Stats.newBuilder()
                        .addMatchStat(GameStateApiOutboundStats.Stats.Stat.newBuilder()
                                .setParticipantRef(HOME_EXTERNAL_ID)
                                .setType(GameStateApiOutboundStats.Stats.Stat.SportStatType.newBuilder()
                                        .setSoccerStatType(GameStateApiOutboundSportSoccer.SoccerStatType.BALL_POSSESSION))
                                .setValue(0.6))
                        .addPeriodStat(GameStateApiOutboundStats.Stats.PeriodStat.newBuilder()
                                .setPeriod(GameStateApiOutboundStats.Stats.PeriodStat.Period.newBuilder()
                                        .setSoccerPeriod(GameStateApiOutboundSportSoccer.SoccerPeriod.FIRST_HALF))
                                .setStat(GameStateApiOutboundStats.Stats.Stat.newBuilder()
                                        .setParticipantRef(AWAY_EXTERNAL_ID)
                                        .setType(GameStateApiOutboundStats.Stats.Stat.SportStatType.newBuilder()
                                                .setSoccerStatType(GameStateApiOutboundSportSoccer.SoccerStatType.GOALS))
                                        .setValue(1)))
                        .addParticipantStat(GameStateApiOutboundStats.Stats.Stat.newBuilder()
                                .setParticipantRef(HOME_PLAYER_ID)
                                .setType(GameStateApiOutboundStats.Stats.Stat.SportStatType.newBuilder()
                                        .setSoccerStatType(GameStateApiOutboundSportSoccer.SoccerStatType.YELLOW_CARDS))
                                .setValue(1)))
                .addAllParticipants(Arrays
                        .asList(
                                GameStateApiOutboundParticipant.Participant.newBuilder()
                                        .setType(GameStateApiOutboundParticipant.Participant.ParticipantType.TEAM)
                                        .setId(HOME_EXTERNAL_ID)
                                        .setName("Chelsea F.C")
                                        .setInfo(GameStateApiOutboundParticipant.Participant.ParticipantInfo.newBuilder()
                                                .setTeamAttributes(GameStateApiOutboundParticipant.Participant.ParticipantInfo.TeamAttributes.newBuilder()
                                                        .setSoccerTeamAttributes(GameStateApiOutboundSportSoccer.SoccerTeamAttributes.newBuilder()
                                                                .setJerseyColour("#0000FF,#FFFFFF")
                                                                .build())
                                                        .build())
                                                .build())
                                        .build(),
                                GameStateApiOutboundParticipant.Participant.newBuilder()
                                        .setType(GameStateApiOutboundParticipant.Participant.ParticipantType.TEAM)
                                        .setId(AWAY_EXTERNAL_ID)
                                        .setName("Manchester United F.C.")
                                        .setInfo(GameStateApiOutboundParticipant.Participant.ParticipantInfo.newBuilder()
                                                .setTeamAttributes(GameStateApiOutboundParticipant.Participant.ParticipantInfo.TeamAttributes.newBuilder()
                                                        .setSoccerTeamAttributes(GameStateApiOutboundSportSoccer.SoccerTeamAttributes.newBuilder()
                                                                .setJerseyColour("#FF0000,#FFFFFF")
                                                                .build())
                                                        .build())
                                                .build())
                                        .build(),
                                GameStateApiOutboundParticipant.Participant.newBuilder()
                                        .setType(GameStateApiOutboundParticipant.Participant.ParticipantType.PLAYER)
                                        .setInfo(GameStateApiOutboundParticipant.Participant.ParticipantInfo.newBuilder()
                                                .setPlayerAttributes(GameStateApiOutboundParticipant.Participant.ParticipantInfo.PlayerAttributes.newBuilder()
                                                        .setSoccerPlayerAttributes(GameStateApiOutboundSportSoccer.SoccerPlayerAttributes.newBuilder()
                                                                .setPosition("Goalkeeper")
                                                                .setPlayerPosition(GameStateApiOutboundSportSoccer.SoccerPlayerAttributes.SoccerPlayerPosition.MIDFIELDER)
                                                                .setParentParticipantRef(HOME_EXTERNAL_ID)
                                                                .setShirtNumber(10)
                                                                .setStartingPosition(GameStateApiOutboundSportSoccer.SoccerPlayerAttributes.SoccerPlayerStartingPositionType.BENCH)
                                                                .build())
                                                        .build())
                                                .build())
                                        .setName("Tester")
                                        .build()))
                .addIncidents(GameStateApiOutboundIncident.Incident.newBuilder()
                        .setVersion(1)
                        .setId(incidentID)
                        .setType(GameStateApiOutboundIncident.Incident.IncidentType.newBuilder()
                                .setSoccerIncidentType(GameStateApiOutboundSportSoccer.SoccerIncidentType.UNCONFIRMED_SHOT))
                        .addExternalRef(GameStateApiOutboundCommon.ExternalRef.newBuilder()
                                .setExternalId("1")
                                .setProvider(GameStateApiOutboundCommon.Provider.OPTA))
                        .addParticipantRef(HOME_EXTERNAL_ID)
                        .addParticipantRef(HOME_PLAYER_ID)
                        .setTime(GameStateApiOutboundIncident.Incident.IncidentTime.newBuilder()
                                .setSoccerIncidentTime(GameStateApiOutboundSportSoccer.SoccerIncidentTime.newBuilder()
                                        .setPeriod(GameStateApiOutboundSportSoccer.SoccerPeriod.FIRST_HALF)
                                        .setActualTime(Timestamp.newBuilder().setSeconds(testTime))
                                        .setClock(GameStateApiOutboundSportSoccer.SoccerIncidentTime.Clock.newBuilder()
                                                .setMinutes(5)
                                                .setMinutes(10))))
                        .setQualifiers(GameStateApiOutboundIncident.Incident.IncidentQualifiers.newBuilder()
                                .setSoccerIncidentQualifiers(GameStateApiOutboundSportSoccer.SoccerIncidentQualifiers.newBuilder()
                                        .addQualifiers(GameStateApiOutboundSportSoccer.SoccerIncidentQualifiers.SoccerIncidentQualifier.newBuilder()
                                        .setQualifier(GameStateApiOutboundSportSoccer.SoccerIncidentQualifiers.SoccerIncidentQualifier.SoccerQualifier.ASSISTANT_PARTICIPANT_REF)
                                        .setValue("3003"))))
                        .setLastModified(Timestamp.newBuilder().setSeconds(testTime)));
    }

    private static GameStateApiOutboundMeta.Meta.FeedStatus buildFeedStatus() {
        return GameStateApiOutboundMeta.Meta.FeedStatus.newBuilder()
                .setProvider(GameStateApiOutboundCommon.Provider.OPTA)
                .setFeatures(buildFeedFeatures())
                .build();
    }

    private static GameStateApiOutboundMeta.Meta.FeedFeatures buildFeedFeatures() {
        return GameStateApiOutboundMeta.Meta.FeedFeatures.newBuilder()

                .addSupportedIncidentTypes(GameStateApiOutboundIncident.Incident.IncidentType.newBuilder()
                        .setSoccerIncidentType(GameStateApiOutboundSportSoccer.SoccerIncidentType.GOAL).build())
                .addSupportedIncidentTypes(GameStateApiOutboundIncident.Incident.IncidentType.newBuilder()
                        .setSoccerIncidentType(GameStateApiOutboundSportSoccer.SoccerIncidentType.DANGEROUS_ATTACK).build())
                .addSupportedIncidentTypes(GameStateApiOutboundIncident.Incident.IncidentType.newBuilder()
                        .setSoccerIncidentType(GameStateApiOutboundSportSoccer.SoccerIncidentType.FREE_KICK).build())

                .addSupportedStatTypes(GameStateApiOutboundStats.Stats.Stat.SportStatType.newBuilder()
                        .setSoccerStatType(GameStateApiOutboundSportSoccer.SoccerStatType.BALL_POSSESSION).build())
                .addSupportedStatTypes(GameStateApiOutboundStats.Stats.Stat.SportStatType.newBuilder()
                        .setSoccerStatType(GameStateApiOutboundSportSoccer.SoccerStatType.GOALS).build())
                .addSupportedStatTypes(GameStateApiOutboundStats.Stats.Stat.SportStatType.newBuilder()
                        .setSoccerStatType(GameStateApiOutboundSportSoccer.SoccerStatType.YELLOW_CARDS).build())
                .build();
    }
}
