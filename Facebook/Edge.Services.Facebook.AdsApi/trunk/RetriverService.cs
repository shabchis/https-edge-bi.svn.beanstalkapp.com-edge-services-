using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Edge.Core.Services;
using Edge.Data.Pipeline;
using MyFacebook=Facebook;

namespace Edge.Services.Facebook.AdsApi
{
	//class RetriverService : PipelineService
	//{
	//    private MyFacebook.Rest.Api _facebookAPI;
	//    private MyFacebook.Session.ConnectSession _connSession;
	//    protected override ServiceOutcome DoWork()
	//    {
	//        //TODO: TALK WITH DORON I THINK ITS IS PARENTINSTANCEID
	//        this.Delivery = new Delivery(this.Instance.InstanceID)
	//        {
	//            TargetPeriod = this.TargetPeriod
	//        };

	//        //connect face book settings
	//        _connSession = new MyFacebook.Session.ConnectSession(this.Delivery.Parameters["APIKey"].ToString(), this.Delivery.Parameters["applicationSecret"].ToString());
	//        _connSession.SessionKey = this.Delivery.Parameters["sessionKey"].ToString();
	//        _connSession.SessionSecret = this.Delivery.Parameters["sessionSecret"].ToString();
	//        _facebookAPI = new MyFacebook.Rest.Api(_connSession);
	//        _facebookAPI.Auth.Session.SessionExpires = false;

	//        foreach (DeliveryFile file in this.Delivery.Files)
	//        {
	//            //TODO: TALK WITH DORON HOW WE ARE DOING IT
	//            //
	//        }


	//        return ServiceOutcome.Success;
	//    }

	//}
}
