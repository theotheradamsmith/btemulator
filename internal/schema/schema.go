package schema

const (
	Instance                           = "uapl-dev-devicemgmt"
	Project                            = "smoothwall-sandbox"
	TableName                          = "UaplDevices"
	ColumnFamilyFirebaseProperties     = "FirebaseProperties"
	ColumnFamilyDeviceProperties       = "DeviceProperties"
	ColumnFamilyRegistrationProperties = "RegistrationProperties"
	ColumnFCM                          = "FcmToken"
	ColumnDID                          = "DeviceId"
	ColumnAID                          = "AdoptionId"
	ColumnAppK                         = "ApplianceKey"
	ColumnAuthToken                    = "AuthTokens"
	ColumnMainKey                      = "MainKey"
	ColumnChallenge                    = "Challenge"
	ColumnCreated                      = "CreatedDate"
	ColumnRegistered                   = "Registered"
	ColumnTrusted                      = "Trusted"
)

var ColumnFamilies = []string{ColumnFamilyFirebaseProperties, ColumnFamilyDeviceProperties, ColumnFamilyRegistrationProperties}

//var devicePropertyColumnNames = []string{columnDID, columnAID, columnAppK, columnAuthToken, columnTrusted, columnCreated}

type DeviceEntry struct {
	AID string
	QID string
	DID string
	FCM string
}

var Devices = []DeviceEntry{
	{
		AID: "aid-1",
		QID: "qid-1",
		DID: "did-1",
		FCM: "fcm-1",
	},
	{
		AID: "aid-2",
		QID: "qid-2",
		DID: "did-2",
		FCM: "fcm-2",
	},
}
