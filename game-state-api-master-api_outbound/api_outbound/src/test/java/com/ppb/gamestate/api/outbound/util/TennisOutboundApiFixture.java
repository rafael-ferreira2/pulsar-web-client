package com.ppb.gamestate.api.outbound.util;

import com.google.protobuf.Timestamp;
import com.ppb.gamestate.api.outbound.GameStateOutbound;
import com.ppb.gamestate.api.outbound.common.GameStateApiOutboundCommon;
import com.ppb.gamestate.api.outbound.common.GameStateApiOutboundCommon.BooleanValue;
import com.ppb.gamestate.api.outbound.incident.GameStateApiOutboundIncident.Incident.IncidentQualifiers;
import com.ppb.gamestate.api.outbound.incident.GameStateApiOutboundIncident.Incident.IncidentType;
import com.ppb.gamestate.api.outbound.participant.GameStateApiOutboundParticipant;
import com.ppb.gamestate.api.outbound.sport.tennis.GameStateApiOutboundSportTennis.TennisIncidentQualifiers.TennisIncidentQualifier;
import com.ppb.gamestate.api.outbound.sport.tennis.GameStateApiOutboundSportTennis.TennisIncidentQualifiers.TennisIncidentQualifier.TennisQualifier;

import static com.ppb.gamestate.api.outbound.fixture.GameStateApiOutboundFixture.Fixture;
import static com.ppb.gamestate.api.outbound.incident.GameStateApiOutboundIncident.Incident;
import static com.ppb.gamestate.api.outbound.meta.GameStateApiOutboundMeta.Meta;
import static com.ppb.gamestate.api.outbound.meta.GameStateApiOutboundMeta.Meta.InstructionType.FULL_UPDATE;
import static com.ppb.gamestate.api.outbound.participant.GameStateApiOutboundParticipant.Participant.ParticipantInfo.TeamAttributes;
import static com.ppb.gamestate.api.outbound.participant.GameStateApiOutboundParticipant.Participant.ParticipantInfo.newBuilder;
import static com.ppb.gamestate.api.outbound.sport.tennis.GameStateApiOutboundSportTennis.*;

public class TennisOutboundApiFixture {

    public static final Timestamp LAST_MODIFIED_TIMESTAMP = Timestamp.newBuilder().setSeconds(1590874).setNanos(325000000).build();
    public static final Timestamp LAST_MODIFIED_TIMESTAMP_2 = Timestamp.newBuilder().setSeconds(1598874).setNanos(325123000).build();
    public static final Timestamp SET_1_START_TIMESTAMP = Timestamp.newBuilder().setSeconds(1590874).setNanos(325123000).build();
    public static final Timestamp SET_2_START_TIMESTAMP = Timestamp.newBuilder().setSeconds(1598874).setNanos(325123000).build();
    public static final Timestamp ACTUAL_START_TIME_TIMESTAMP = Timestamp.newBuilder().setSeconds(1598874).setNanos(525000000).build();
    public static final Timestamp EXPECTED_START_TIME_TIMESTAMP = Timestamp.newBuilder().setSeconds(1588874).setNanos(425000000).build();
    public static final Timestamp EXPECTED_END_TIME_TIMESTAMP = Timestamp.newBuilder().setSeconds(1588874).setNanos(825000000).build();

    public static final String TEST_INGRESS_CORRELATION_ID = "2012973-12836739";
    public static final String EXTERNAL_TEAM_ID_1 = "234";
    public static final String EXTERNAL_TEAM_ID_2 = "456";
    public static final String EXTERNAL_TEAM_NAME_1 = "Naomi Osaka";
    public static final String EXTERNAL_TEAM_NAME_2 = "Serena Williams";
    public static final String PARTICIPANT_KEY_1 = "nosaka";
    public static final String PARTICIPANT_KEY_2 = "swilliams";
    public static final String EXTERNAL_MATCH_ID = "external-match-id";
    public static final String RAMP_MATCH_ID = "ramp-match-id";
    public static final String TEST_SET_START_INCIDENT_ID_1 = "set-start-incident-1";
    public static final String TEST_SET_START_INCIDENT_ID_2 = "set-start-incident-2";
    public static final String TEST_INCIDENT_ID_1 = "test-incident-1";
    public static final String TEST_INCIDENT_ID_2 = "test-incident-2";
    public static final String TEST_INCIDENT_ID_3 = "test-incident-3";
    public static final String TEST_INCIDENT_ID_4 = "test-incident-4";
    public static final String TEST_INCIDENT_ID_5 = "test-incident-5";
    public static final String TEST_INCIDENT_ID_6 = "test-incident-6";
    public static final String RAMP_COMPETITION_ID = "ramp-competition-id";
    public static final String TEST_SERVING_PLAYER_A = "IMG-234";
    public static final String TEST_SERVING_PLAYER_B = "IMG-456";


    public static GameStateOutbound.Api create() {
        return GameStateOutbound.Api.newBuilder()
                .setMeta(createMeta())
                .addParticipants(createParticipant(EXTERNAL_TEAM_ID_1, EXTERNAL_TEAM_NAME_1, 1, PARTICIPANT_KEY_1))
                .addParticipants(createParticipant(EXTERNAL_TEAM_ID_2, EXTERNAL_TEAM_NAME_2, 2, PARTICIPANT_KEY_2))
                .setFixture(createTennisFixture(EXTERNAL_MATCH_ID, RAMP_MATCH_ID))
                .addIncidents(createPointIncident(TEST_INCIDENT_ID_1, TennisIncidentType.POINT_RALLY,
                        internalTeamId(EXTERNAL_TEAM_ID_1), 2, 5, internalTeamId(EXTERNAL_TEAM_ID_1),
                        1, 0, TEST_SERVING_PLAYER_A)
                )
                .addIncidents(createPointIncident(TEST_INCIDENT_ID_2, TennisIncidentType.POINT_ACE,
                        internalTeamId(EXTERNAL_TEAM_ID_1), 1, 2, internalTeamId(EXTERNAL_TEAM_ID_1),
                        2, 0, TEST_SERVING_PLAYER_B))
                .addIncidents(createSetStartIncident(TEST_SET_START_INCIDENT_ID_1, 1, SET_1_START_TIMESTAMP,LAST_MODIFIED_TIMESTAMP))
                .addIncidents(createSetStartIncident(TEST_SET_START_INCIDENT_ID_2, 2, SET_2_START_TIMESTAMP,LAST_MODIFIED_TIMESTAMP_2))
                .addIncidents(createMatchEventIncident(TEST_INCIDENT_ID_3, TennisIncidentType.MATCH_RESUMED))
                .addIncidents(createMatchEventIncident(TEST_INCIDENT_ID_4, TennisIncidentType.HEAT_DELAY))
                .addIncidents(createMatchEventIncident(TEST_INCIDENT_ID_5, TennisIncidentType.SIDE_SWAP_GAME))
                .addIncidents(createRetirementIncident(TEST_INCIDENT_ID_6, TennisIncidentType.RETIREMENT, internalTeamId(EXTERNAL_TEAM_ID_1)))
                .build();
    }


    public static GameStateApiOutboundParticipant.Participant createParticipant(String externalId, String name, int ranking, String participantKey) {
        return GameStateApiOutboundParticipant.Participant.newBuilder()
                .setId(internalTeamId(externalId))
                .setName(name)
                .setType(GameStateApiOutboundParticipant.Participant.ParticipantType.TEAM)
                .addExternalRef(GameStateApiOutboundCommon.ExternalRef.newBuilder()
                        .setProvider(GameStateApiOutboundCommon.Provider.AMELCO)
                        .setExternalId(externalId)
                        .build())
                .setInfo(newBuilder()
                        .setTeamAttributes(TeamAttributes.newBuilder()
                                .setTennisTeamAttributes(TennisTeamAttributes.newBuilder()
                                        .setRank(ranking)
                                        .setParticipantKey(participantKey)
                                        .build())
                                .build())
                        .build())
                .build();
    }

    public static String internalTeamId(String externalId) {
        return "internal-team-id/" + externalId;
    }

    public static Incident createSetStartIncident(String id, int setNumber, Timestamp setStartTime, Timestamp lastModified) {
        return Incident.newBuilder()
                .setId(id)
                .setTime(Incident.IncidentTime.newBuilder()
                        .setTennisIncidentTime(TennisIncidentTime.newBuilder()
                                .setTimestamp(setStartTime)
                                .build()).build())
                .setLastModified(lastModified)
                .setType(IncidentType.newBuilder()
                        .setTennisIncidentType(TennisIncidentType.SET_START)
                        .build())
                .setQualifiers(IncidentQualifiers.newBuilder()
                        .setTennisIncidentQualifiers(TennisIncidentQualifiers
                                .newBuilder()
                                .addQualifiers(TennisIncidentQualifier.newBuilder()
                                        .setQualifier(TennisQualifier.SET_NUMBER)
                                        .setValue(String.valueOf(setNumber))
                                        .build())
                                .build())
                        .build())
                .build();
    }

    public static Incident createMatchEventIncident(String id, TennisIncidentType type) {
        return Incident.newBuilder()
                .setId(id)
                .setLastModified(LAST_MODIFIED_TIMESTAMP)
                .setType(IncidentType.newBuilder()
                        .setTennisIncidentType(type)
                        .build())
                .build();
    }

    public static Incident createRetirementIncident(String id, TennisIncidentType type, String retiredParticipantRef) {
        return Incident.newBuilder()
                .setId(id)
                .setLastModified(LAST_MODIFIED_TIMESTAMP)
                .setType(IncidentType.newBuilder()
                        .setTennisIncidentType(type)
                        .build())
                .setQualifiers(IncidentQualifiers.newBuilder()
                        .setTennisIncidentQualifiers(TennisIncidentQualifiers
                                .newBuilder()
                                .addQualifiers(TennisIncidentQualifier.newBuilder()
                                        .setQualifier(TennisQualifier.RETIRED_PARTICIPANT_REF)
                                        .setValue(retiredParticipantRef)
                                        .build())
                                .build())

                        .build())
                .build();
    }


    public static Incident createPointIncident(String id, TennisIncidentType type,
                                               String participantRef,
                                               int serveNumber, int pointNumber,
                                               String winnerRef,
                                               int teamAScores, int teamBScores, String servingPlayer) {
        return Incident.newBuilder()
                .setId(id)
                .addParticipantRef(participantRef)
                .setLastModified(LAST_MODIFIED_TIMESTAMP)
                .setType(IncidentType.newBuilder()
                        .setTennisIncidentType(type)
                        .build())
                .setQualifiers(IncidentQualifiers.newBuilder()
                        .setTennisIncidentQualifiers(TennisIncidentQualifiers
                                .newBuilder()
                                .addQualifiers(TennisIncidentQualifier.newBuilder()
                                        .setQualifier(TennisQualifier.SERVE_NUMBER)
                                        .setValue(String.valueOf(serveNumber))
                                        .build())
                                .addQualifiers(TennisIncidentQualifier.newBuilder()
                                        .setQualifier(TennisQualifier.POINT_NUMBER)
                                        .setValue(String.valueOf(pointNumber))
                                        .build())
                                .addQualifiers(TennisIncidentQualifier.newBuilder()
                                        .setQualifier(TennisQualifier.SERVING_PARTICIPANT_REF)
                                        .setValue(servingPlayer)
                                        .build())
                                .addQualifiers(TennisIncidentQualifier.newBuilder()
                                        .setQualifier(TennisQualifier.POINT_WINNER_PARTICIPANT_REF)
                                        .setValue(winnerRef)
                                        .build())
                                .addQualifiers(TennisIncidentQualifier.newBuilder()
                                        .setQualifier(TennisQualifier.SET_NUMBER)
                                        .setValue("3")
                                        .build())
                                .addQualifiers(TennisIncidentQualifier.newBuilder()
                                        .setQualifier(TennisQualifier.GAME_NUMBER)
                                        .setValue("4")
                                        .build())
                                .addQualifiers(TennisIncidentQualifier.newBuilder()
                                        .setQualifier(TennisQualifier.TEAM_A_POINTS)
                                        .setValue(String.valueOf(teamAScores))
                                        .build())
                                .addQualifiers(TennisIncidentQualifier.newBuilder()
                                        .setQualifier(TennisQualifier.TEAM_B_POINTS)
                                        .setValue(String.valueOf(teamBScores))
                                        .build())
                                .addQualifiers(TennisIncidentQualifier.newBuilder()
                                        .setQualifier(TennisQualifier.TEAM_A_TIE_BREAK_POINTS)
                                        .setValue(String.valueOf(1))
                                        .build())
                                .addQualifiers(TennisIncidentQualifier.newBuilder()
                                        .setQualifier(TennisQualifier.TEAM_B_TIE_BREAK_POINTS)
                                        .setValue(String.valueOf(2))
                                        .build())
                                .addQualifiers(TennisIncidentQualifier.newBuilder()
                                        .setQualifier(TennisQualifier.TEAM_A_SETS_WON)
                                        .setValue(String.valueOf(1))
                                        .build())
                                .addQualifiers(TennisIncidentQualifier.newBuilder()
                                        .setQualifier(TennisQualifier.TEAM_B_SETS_WON)
                                        .setValue(String.valueOf(2))
                                        .build())
                                .addQualifiers(TennisIncidentQualifier.newBuilder()
                                        .setQualifier(TennisQualifier.TEAM_A_GAMES_WON)
                                        .setValue(String.valueOf(2))
                                        .build())
                                .addQualifiers(TennisIncidentQualifier.newBuilder()
                                        .setQualifier(TennisQualifier.TEAM_B_GAMES_WON)
                                        .setValue(String.valueOf(2))
                                        .build())
                                .build())

                        .build())
                .build();
    }

    public static Meta createMeta() {
        return Meta.newBuilder()
                .setGameStateId("test-gamestate-id")
                .setHighWaterMark(1)
                .setInstructionType(FULL_UPDATE)
                .setVersion("test-version-1")
                .setIngressCorrelationId(TEST_INGRESS_CORRELATION_ID)
                .setSport(Meta.Sport.TENNIS)
                .setGameStatePreviousId("")
                .build();
    }

    public static Fixture createTennisFixture(String externalMatchId, String rampId) {
        return Fixture.newBuilder()
                .setSportsBookId("test-sportbook-id")
                .setActualStartTime(ACTUAL_START_TIME_TIMESTAMP)
                .setExpectedStartTime(EXPECTED_START_TIME_TIMESTAMP)
                .setExpectedEndTime(EXPECTED_END_TIME_TIMESTAMP)

                .setStatus(Fixture.FixtureStatus.PRE_MATCH)
                .setAttributes(Fixture.FixtureAttributes.newBuilder()
                        .setTennisAttributes(createTennisAttributes())
                        .build())
                .addExternalRef(GameStateApiOutboundCommon.ExternalRef.newBuilder()
                        .setExternalId(rampId)
                        .setProvider(GameStateApiOutboundCommon.Provider.RAMP)
                        .build())
                .addExternalRef(GameStateApiOutboundCommon.ExternalRef.newBuilder()
                        .setExternalId(externalMatchId)
                        .setProvider(GameStateApiOutboundCommon.Provider.AMELCO)
                        .build())
                .build();
    }

    public static TennisAttributes createTennisAttributes() {
        return TennisAttributes.newBuilder()
                .setMatchType(MatchType.SINGLES)
                .setParticipantGender(Gender.MALE)
                .setAdvantagePlayedNormalGame(BooleanValue.FALSE)
                .setAdvantagePlayedTieBreakGame(BooleanValue.FALSE)
                .setNumberOfSets(3)
                .setLastSetFormat(LastSetFormat.NORMAL_WITH_TIEBREAK)
                .setGamesInSetFirstTo(6)
                .setSurface("Grass")
                .setTieBreakPlayedAfterXGames(6)
                .setPointsInTieBreakFirstTo(7)
                .setPointsInRaceSetFirstTo(10)
                .setTournamentType("ITF")
                .setCurrentSet(2)
                .setCurrentGame(6)
                .setMatchCurrentScore("6-4 3-2 0-3*")
                .setInTieBreak(BooleanValue.FALSE)
                .setCurrentServeNumber(2)
                .setStartOfMatchServerRef(internalTeamId(EXTERNAL_TEAM_ID_1))
                .setCurrentServerRef(internalTeamId(EXTERNAL_TEAM_ID_2))
                .setNextPointServerGender(Gender.MALE)
                .addCompetition(Competition.newBuilder()
                        .setCompetitionName("|ITF Kofu|")
                        .setCompetitionId(RAMP_COMPETITION_ID)
                        .setProvider(GameStateApiOutboundCommon.Provider.RAMP)
                        .build())
                .build();
    }
}
